/**
 * ============================================================
 *  Bohae ESP32 Counter Publisher (PCNT + MQTT)
 * ============================================================
 *
 * ESP32 → Wi-Fi → MQTT/TLS(8883) → bohae_mosquitto → ingest → DB
 *
 * 레포 계약:
 *   - ESP32는 HTTP API로 직접 보내지 않는다. MQTT로만 보낸다.
 *   - topic: bohae/v1/line/<LINE_ID>/counter
 *   - 필수 키: device_id, seq, ts_ms, total_count
 *
 * PCNT 하드웨어 펄스 카운터 (300~600 BPM 정확)
 * 5초마다 publish
 *
 * ★ CONFIG 섹션만 수정하면 현장 배포 가능 ★
 * ============================================================
 */

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include "driver/pulse_cnt.h"
#include "esp_mac.h"

// ===================================================================
//  ★★★  CONFIG — 목요일에 이것만 바꾸면 됨  ★★★
// ===================================================================
const char* WIFI_SSID_CFG     = "생산time";             // 현장 Wi-Fi
const char* WIFI_PASSWORD_CFG = "[REDACTED_WIFI_PW]";            // Wi-Fi 비번
const char* MQTT_HOST_CFG     = "[REDACTED_BROKER_IP]";      // 맥미니 공인 IP
const int   MQTT_PORT_CFG     = 8883;                 // TLS
const char* MQTT_USER_CFG     = "bohae_mqtt";
const char* MQTT_PASS_CFG     = "[REDACTED_MQTT_PW]";  // 서버 .env 기준
const char* LINE_ID_CFG       = "LINE_JSNG_B1_02";    // canonical line_id (제조 1동 2호기)
const int   COUNTER_GPIO_CFG  = 5;                     // Nano ESP32 D2 = GPIO 5
const int   PUBLISH_MS        = 5000;                  // 5초
const int   TARGET_QTY_CFG    = 0;
// ===================================================================

// PCNT
#define PCNT_HIGH_LIMIT  30000
#define PCNT_LOW_LIMIT  -1
#define GLITCH_NS        1000

// 전역
WiFiClientSecure tlsClient;
PubSubClient mqtt(tlsClient);

pcnt_unit_handle_t pcntUnit = NULL;
volatile int64_t totalCount = 0;
int lastPcntVal = 0;

uint32_t seq = 0;
String deviceId;
String bootId;
String topic;

unsigned long lastPubMs  = 0;
unsigned long lastWifiMs = 0;

// ===== ID =====
String macSuffix() {
  uint8_t mac[6];
  esp_read_mac(mac, ESP_MAC_WIFI_STA);
  char buf[18];
  snprintf(buf, sizeof(buf), "%02X%02X%02X%02X%02X%02X",
           mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  return String(buf);
}

String makeDeviceId() {
  String m = macSuffix();
  return String("esp32-counter-") + m.substring(m.length() - 6);
}

String makeBootId() {
  char buf[16];
  snprintf(buf, sizeof(buf), "boot-%08X", esp_random());
  return String(buf);
}

// ===== PCNT =====
void initPCNT() {
  pcnt_unit_config_t uc = {};
  uc.high_limit = PCNT_HIGH_LIMIT;
  uc.low_limit  = PCNT_LOW_LIMIT;
  ESP_ERROR_CHECK(pcnt_new_unit(&uc, &pcntUnit));

  pcnt_glitch_filter_config_t fc = {};
  fc.max_glitch_ns = GLITCH_NS;
  ESP_ERROR_CHECK(pcnt_unit_set_glitch_filter(pcntUnit, &fc));

  pcnt_chan_config_t cc = {};
  cc.edge_gpio_num  = COUNTER_GPIO_CFG;
  cc.level_gpio_num = -1;
  pcnt_channel_handle_t ch = NULL;
  ESP_ERROR_CHECK(pcnt_new_channel(pcntUnit, &cc, &ch));

  ESP_ERROR_CHECK(pcnt_channel_set_edge_action(ch,
    PCNT_CHANNEL_EDGE_ACTION_INCREASE,
    PCNT_CHANNEL_EDGE_ACTION_HOLD));
  ESP_ERROR_CHECK(pcnt_channel_set_level_action(ch,
    PCNT_CHANNEL_LEVEL_ACTION_KEEP,
    PCNT_CHANNEL_LEVEL_ACTION_KEEP));

  ESP_ERROR_CHECK(pcnt_unit_enable(pcntUnit));
  ESP_ERROR_CHECK(pcnt_unit_clear_count(pcntUnit));
  ESP_ERROR_CHECK(pcnt_unit_start(pcntUnit));
  Serial.println("[PCNT] OK gpio=" + String(COUNTER_GPIO_CFG));
}

int readPCNT() {
  int cur = 0;
  pcnt_unit_get_count(pcntUnit, &cur);
  int delta = cur - lastPcntVal;
  if (delta < 0) delta = cur + (PCNT_HIGH_LIMIT - lastPcntVal);
  totalCount += delta;
  lastPcntVal = cur;
  return delta;
}

// ===== Wi-Fi =====
void wifiConnect() {
  if (WiFi.status() == WL_CONNECTED) return;
  Serial.print("[WiFi] -> ");
  Serial.println(WIFI_SSID_CFG);
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(WIFI_SSID_CFG, WIFI_PASSWORD_CFG);
  int t = 0;
  while (WiFi.status() != WL_CONNECTED && t < 20) { delay(500); Serial.print("."); t++; }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println();
    Serial.print("[WiFi] OK ip=");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\n[WiFi] FAIL");
  }
}

// ===== MQTT =====
void mqttConnect() {
  if (mqtt.connected()) return;
  if (WiFi.status() != WL_CONNECTED) return;
  Serial.printf("[MQTT] -> %s:%d\n", MQTT_HOST_CFG, MQTT_PORT_CFG);
  mqtt.setServer(MQTT_HOST_CFG, MQTT_PORT_CFG);
  mqtt.setBufferSize(512);
  String cid = "esp32_" + deviceId;
  if (mqtt.connect(cid.c_str(), MQTT_USER_CFG, MQTT_PASS_CFG)) {
    Serial.println("[MQTT] OK id=" + cid);
  } else {
    Serial.printf("[MQTT] FAIL rc=%d\n", mqtt.state());
  }
}

// ===== NTP =====
void syncNTP() {
  configTzTime("KST-9", "pool.ntp.org", "time.google.com");
  Serial.println("[NTP] sync");
}

int64_t epochMs() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (int64_t)tv.tv_sec * 1000LL + (int64_t)tv.tv_usec / 1000LL;
}

// ===== Publish =====
void publish(int pulse) {
  if (!mqtt.connected()) return;
  seq++;
  int64_t ts = epochMs();
  const char* st = (pulse > 0) ? "RUNNING" : "IDLE";

  JsonDocument doc;
  doc["device_id"]     = deviceId;
  doc["boot_id"]       = bootId;
  doc["seq"]           = seq;
  doc["line_id"]       = LINE_ID_CFG;
  doc["sensor_kind"]   = "COUNTER";
  doc["ts_ms"]         = ts;
  doc["window_ms"]     = PUBLISH_MS;
  doc["pulse_count"]   = pulse;
  doc["total_count"]   = (long long)totalCount;
  doc["target_qty"]    = TARGET_QTY_CFG;
  doc["quality"]       = "OK";
  doc["runtime_state"] = st;
  doc["status"]        = st;

  char buf[512];
  serializeJson(doc, buf, sizeof(buf));
  bool ok = mqtt.publish(topic.c_str(), buf, false);

  Serial.printf("[PUB] seq=%u pulse=%d total=%lld %s\n",
    seq, pulse, (long long)totalCount, ok ? "OK" : "FAIL");
}

// ===== Setup =====
void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("========================================");
  Serial.println(" Bohae ESP32 Counter (PCNT + MQTT/TLS)");
  Serial.println("========================================");

  deviceId = makeDeviceId();
  bootId   = makeBootId();
  topic    = String("bohae/v1/line/") + LINE_ID_CFG + "/counter";

  Serial.println("device_id = " + deviceId);
  Serial.println("boot_id   = " + bootId);
  Serial.println("topic     = " + topic);
  Serial.println("broker    = " + String(MQTT_HOST_CFG) + ":" + String(MQTT_PORT_CFG));
  Serial.println("gpio      = " + String(COUNTER_GPIO_CFG));
  Serial.println("interval  = " + String(PUBLISH_MS) + "ms");

  pinMode(COUNTER_GPIO_CFG, INPUT_PULLDOWN);
  initPCNT();

  // TLS: 자체서명 인증서 허용 (나중에 setCACert로 핀닝 전환 가능)
  tlsClient.setInsecure();

  wifiConnect();
  syncNTP();
  mqttConnect();
  lastPubMs = millis();
}

// ===== Loop =====
void loop() {
  unsigned long now = millis();

  // Wi-Fi 재접속 (10초)
  if (now - lastWifiMs >= 10000) {
    lastWifiMs = now;
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[WiFi] reconnect...");
      wifiConnect();
      if (WiFi.status() == WL_CONNECTED) syncNTP();
    }
  }

  // MQTT 재접속
  if (!mqtt.connected()) mqttConnect();
  mqtt.loop();

  // 5초마다 publish (pulse=0 이어도 하트비트)
  if (now - lastPubMs >= (unsigned long)PUBLISH_MS) {
    lastPubMs = now;
    int pulse = readPCNT();
    publish(pulse);
  }
}
