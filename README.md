<p align="center">
  <strong>🏭</strong>
</p>

<h1 align="center">보해양조 제성공장 — 스마트팩토리 구축</h1>

<p align="center">
  <em>"제조 현장을 먼저 경험한 뒤 시스템을 만든 사람의 포트폴리오"</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Next.js-15-000000?style=flat-square&logo=next.js&logoColor=white" />
  <img src="https://img.shields.io/badge/TypeScript-5.x-3178C6?style=flat-square&logo=typescript&logoColor=white" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/OR--Tools-CP--SAT-4285F4?style=flat-square&logo=google&logoColor=white" />
  <img src="https://img.shields.io/badge/ESP32-C++-E7352C?style=flat-square&logo=espressif&logoColor=white" />
  <img src="https://img.shields.io/badge/MQTT-Mosquitto-660066?style=flat-square&logo=eclipsemosquitto&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-Container-2496ED?style=flat-square&logo=docker&logoColor=white" />
</p>

---

## 👤 소개

| | |
|------|------|
| **이름** | 허인회 |
| **학력** | 전남대학교 기계공학과 |
| **현 소속** | 보해양조 제성공장 / 스마트팩토리 AI TF 팀장 (3인) |
| **포지셔닝** | 제조 현장(라인 운영 + 설비 정비) → OT-IT 브리지형 PM/아키텍트 |
| **기간** | 2023.06 ~ 현재 |

---

## 🎯 한 줄 요약

> 주류 제조 공장에서 **라인을 직접 돌리고, 베어링을 갈고, 펌프를 분해**해본 기계공학 전공자가,  
> **APS + MES + IoT + LIMS + EAM + 3D 디지털 트윈 + AI**를 직접 설계·구축한 프로젝트입니다.

---

## 📊 핵심 성과

<table>
<tr>
<td align="center" width="25%">
<h3>116,748</h3>
<sub>IoT 카운터 검증<br/>산업용 카운터와 <strong>정확히 일치</strong></sub>
</td>
<td align="center" width="25%">
<h3>98페이지 · 234 API</h3>
<sub>MES/OPS 플랫폼<br/>52개 서버 모듈</sub>
</td>
<td align="center" width="25%">
<h3>60초</h3>
<sub>APS 스케줄링<br/>14개 라인 × 55일</sub>
</td>
<td align="center" width="25%">
<h3>≈ 3억 → 내재화</h3>
<sub>키오스크/전광판<br/>외주 대비 대폭 절감</sub>
</td>
</tr>
</table>

---

## 🏗️ 시스템 아키텍처

```
┌──────────────────────────────────────────────────────────────┐
│                        현장 (OT Layer)                        │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐  │
│  │ 근접센서  │   │ Coriolis │   │ Magmeter │   │ Pt100    │  │
│  │ BUP-50S  │   │ 유량계   │   │ 유량계   │   │ 온도센서  │  │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘  │
│       └──────────────┴──────────────┴──────────────┘         │
│                    신호 탭 (비파괴 분기)                       │
│                          ↓                                    │
│              ┌─────────────────────┐                          │
│              │  ESP32 + ADC/PCNT   │  ← C++ 펌웨어 직접 개발  │
│              │  (엣지 디바이스)     │                          │
│              └──────────┬──────────┘                          │
└─────────────────────────┼────────────────────────────────────┘
                          │ MQTT (TLS)
┌─────────────────────────┼────────────────────────────────────┐
│                     서버 (IT Layer)                           │
│                          ↓                                    │
│              ┌─────────────────────┐                          │
│              │   PostgreSQL 16     │                          │
│              └──────────┬──────────┘                          │
│                          ↓                                    │
│  ┌────────────────────────────────────────────────────────┐   │
│  │              FastAPI (Python)                          │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐            │   │
│  │  │ APS 엔진 │  │ LIMS API │  │ EAM API  │  ← 234개   │   │
│  │  │ (OR-Tools│  │ (품질)   │  │ (설비)   │    API      │   │
│  │  │  CP-SAT) │  │          │  │          │            │   │
│  │  └──────────┘  └──────────┘  └──────────┘            │   │
│  └────────────────────────┬───────────────────────────────┘  │
│                           ↓                                   │
│  ┌────────────────────────────────────────────────────────┐   │
│  │        Next.js 15 (TypeScript) — 98개 페이지           │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐         │   │
│  │  │  OPS   │ │키오스크│ │  LIMS  │ │  EAM   │         │   │
│  │  │ 콘솔   │ │ /전광판│ │ (품질) │ │ (설비) │         │   │
│  │  └────────┘ └────────┘ └────────┘ └────────┘         │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐         │   │
│  │  │ 3D 트윈│ │AI 콕핏│ │  대시  │ │  알람  │         │   │
│  │  │(R3F)   │ │Text2SQL│ │  보드  │ │       │         │   │
│  │  └────────┘ └────────┘ └────────┘ └────────┘         │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                               │
│  보안: OIDC/PKCE · AES-256-GCM · RBAC 5단계 · OTP · CAPTCHA  │
│  운영: E2E 스모크 · 10h 소크 · Zero-downtime · CI/CD 4개     │
└───────────────────────────────────────────────────────────────┘
```

---

## 🔬 프로젝트 상세

### 1. IoT 생산 카운터 — *"센서부터 직접 설계"*

생산라인의 기존 산업용 센서를 **교체하지 않고 신호를 분기(탭)**하여,  
ESP32 마이크로컨트롤러로 데이터를 수집하는 구조를 **자체 설계**했습니다.

- 아날로그 출력 → ADC → ESP32 전송 구조 직접 설계
- PCNT(Pulse Counter) 하드웨어로 **신호 손실 없는** 수집
- 산업용 카운터(Autonics FX6Y)와 **116,748개 정확히 일치** 검증
- KEYENCE 엔지니어와 대등한 기술 토의 진행

📎 `firmware/esp32_counter_mqtt/esp32_counter_mqtt.ino` — ESP32 C++ 펌웨어

### 2. APS 생산 스케줄링 엔진 — *"수기 계획을 60초로"*

Google OR-Tools CP-SAT 기반으로 **6개 목적함수를 동시 최적화**합니다.

```
minimize (Big-M Lexicographic):
  ① 미배정 최소화     (UNSCHEDULED_COUNT)  ← 최우선
  ② 납기 지연 최소화   (TARDINESS_TOTAL)
  ③ 조기 생산 최소화   (EARLINESS_TOTAL)
  ④ 비선호 라인 최소화  (NONPREFERRED_CNT)
  ⑤ 전환시간 최소화    (SETUP_TOTAL_MIN)
  ⑥ 저속 운전 최소화   (BPM_SLOW_PEN)
```

수요, 설비 제약, 작업자 배치, 휴게시간, 전환시간, CIP 세척을 **동시에 고려**합니다.

📎 `aps-engine/solver/` — 핵심 13개 모듈

### 3. MES/OPS 플랫폼 — *"98페이지, 234 API"*

| 영역 | 구성 |
|------|------|
| **운영 콘솔** (OPS) | 생산계획 승인, 실행, 실적 관리, 의사결정 로그 |
| **키오스크/전광판** | 현장 터치 UI, 실시간 SSE, 48개 컴포넌트 (외주 3억 내재화) |
| **LIMS** (품질) | CCP 워크벤치, HACCP 계획, OOS 처리, SPC 차트, FSMA 204 추적성 |
| **EAM** (설비) | 설비 자산 관리, 점검/보전, 예지보전, OEE/다운타임 분석 |
| **3D 디지털 트윈** | react-three-fiber 기반 공장 3D 모니터링 |
| **AI 콕핏** | Text2SQL, RAG, What-If 시뮬레이션 |
| **SCM/MRP** | 자재소요, 구매 요청, BOM 관리 |
| **대시보드** | OEE, 생산 실적, 알람 현황 |
| **인증/보안** | OIDC/PKCE, AES-256-GCM, RBAC 5단계, OTP, Turnstile |

📎 `api/app.py` — FastAPI 234개 API 전체  
📎 `mes-frontend/` — 페이지, 컴포넌트, 피처, 스토어

### 4. 운영 자동화 — *"배포부터 감시까지"*

- **7단계 E2E 스모크**: 센서 → DB → API → 키오스크 → OPS → 실행 → 1000사이클 소크
- **10시간 소크 테스트** + 워치독 60분 감시
- **Zero-downtime 배포** + onprem CLI (636줄, 원커맨드 운영)
- **GitHub Actions CI/CD** 4개 워크플로우

---

## 📁 레포 구조

```
📂 aps-engine/              ← APS 스케줄링 엔진 (Python)
 ├── 📂 solver/             ★ CP-SAT 최적화 핵심 (13 모듈)
 ├── 📂 loaders/            SSOT 데이터 로더 (Excel/DB)
 ├── 📂 outputs/            결과 출력 (Excel/DB write-back)
 ├── 📂 validators/         계약 검증 게이트
 ├── 📂 postprocess/        감사 보고서
 ├── 📄 config.py           설정 데이터클래스
 └── 📄 main.py             CLI 진입점

📂 api/                     ← FastAPI 백엔드
 └── 📄 app.py              234개 API 라우트 (~2,000줄)

📂 firmware/                ← IoT 엣지
 └── 📄 esp32_counter_mqtt.ino   ESP32 C++ 펌웨어

📂 mes-frontend/            ← Next.js 15 프론트엔드
 ├── 📂 components/         OPS 콘솔, 키오스크, MES 레이아웃
 ├── 📂 features/           OpsConsole V2/V3, 디지털 트윈
 ├── 📂 pages/              대시보드, LIMS, EAM, AI, 알람, TV, 관리자
 ├── 📂 store/              Zustand 상태관리
 └── 📂 lib/                타입, 유틸리티

📂 sql-sample/              ← DB 스키마 샘플 (5개)
📂 tests-sample/            ← 테스트 코드 샘플 (5개)
📂 docs/                    ← 거버넌스 + 도메인 지식
📂 evidence/                ← 현장 검증 사진 (4장)
```

> ⚠️ 이 레포는 포트폴리오용 발췌본입니다. 운영 데이터, DB 스키마 전체, 인증 키 등은 보안상 제외했습니다.

---

## 🛠️ 기술 스택

### Software

| 영역 | 기술 |
|------|------|
| **백엔드** | Python 3.10+, FastAPI, Google OR-Tools CP-SAT |
| **프론트엔드** | Next.js 15 (App Router), TypeScript 5.x, react-three-fiber |
| **데이터베이스** | PostgreSQL 16 |
| **실시간** | Server-Sent Events (SSE), MQTT |
| **인증/보안** | OIDC/PKCE, AES-256-GCM, RBAC 5단계, Cloudflare Turnstile |
| **CI/CD** | GitHub Actions (4 워크플로우), Docker |

### OT / IoT / 센서

| 기술 | 용도 |
|------|------|
| ESP32 (C++) | 엣지 디바이스 — 펌웨어 직접 개발 |
| MQTT / Mosquitto | 센서 → 서버 데이터 전송 |
| Coriolis / Magmeter | 유량 측정 (Phase 2 확장 중) |
| 80GHz Radar | 탱크 레벨 측정 (Phase 2) |
| IO-Link / Modbus TCP | 산업용 통신 (Phase 2) |

### 제조 도메인

| 표준/프레임워크 | 적용 |
|---------------|------|
| ISA-95 | MES 데이터 모델 설계 기준 |
| HACCP / CCP | LIMS 품질 관리 체계 |
| FSMA 204 | FDA 추적성 규제 반영 (LOT 계보) |
| OEE | 설비 가동률 분석 |
| KSMS | 스마트공장 수준진단 (44항목/1000점) |

---

## 👨‍💼 PM / 리더십

| 역할 | 상세 |
|------|------|
| **예산 수립** | 사업계획서 직접 작성, 3회차 분할 발주, SI 대비 50~60% 절감 |
| **임원 보고** | 36장 발표자료 + 임원 대상 사업·기술 브리핑 |
| **벤더 협업** | KEYENCE POC 무상 데모 협상, P&ID 28포인트 센서 선정 |
| **유관 부서** | 전기팀(12장 배선도), 전산팀(12장 아키텍처/보안) 맞춤 브리핑 |
| **현장 실사** | 62항목 체크리스트, KSMS 수준진단, 8 Zone × 28 센서 포인트 |
| **AI 운용** | 4종 에이전트(Codex/Claude/Gemini/ChatGPT) 역할 분담 병렬 운용 |

---

## 📸 현장 검증

### ESP32 + 근접센서(Autonics BUP-50S) 설치
![ESP32 설치](evidence/ESP32%20and%20BUP%2050S.jpeg)

### 산업용 카운터(Autonics FX6Y) — 116,748개 일치
![카운터 일치](evidence/FX6Y%20Autonics%20116748.jpeg)

### 데이터 집계 — 116,748개 정확히 일치 확인
![데이터 일치](evidence/Google%20Sheet%20116748.jpeg)

---

## 📈 진행 현황 (2026-04-24 기준)

Phase 1 파일럿을 완료한 뒤, **17개 도메인 프로젝트**로 범위를 확장해 운영 중입니다.
각 도메인은 독립 스레드로 병렬 진행하며, UI·데이터·인프라 계약은 중앙에서 정합 검수합니다.

### Phase 1 완료
| 상태 | 과제 |
|:---:|------|
| ✅ | IoT 카운터 1호기 파일럿 (Autonics FX6Y 116,748개 일치) |
| ✅ | MES/OPS 98페이지 + APS 엔진 스캐폴드 |
| ✅ | 3D 디지털 트윈 + AI Text2SQL/What-If 프로토타입 |

### 기존 6 프로젝트 — 하드닝 중
| 상태 | 프로젝트 | 현 상황 |
|:---:|------|------|
| 🔄 | **Kiosk** | 모바일 접근성·a11y 지속 하드닝 |
| 🔄 | **Workorder** (인력/근태 통합) | 작업지시·공수 단일 스레드 |
| 🔄 | **Defect** | 불량 원장 계약 정리 |
| 🔄 | **Planning** | 생산 계획 배포 게이트 |
| 🔄 | **Sensor Pipeline** | AWS 텔레메트리 하드닝 |
| 🔄 | **UI 총괄** | 전 도메인 시각 계약 정합 |

### 신규 9 프로젝트 — 2026-04 즉시~순차 개장
| 상태 | 프로젝트 | 범위 |
|:---:|------|------|
| 🆕 | **SOP / Task-Token / 지식 엔진** | 과제·토큰·순위표로 현장 암묵지 디지털화 |
| 🆕 | **Release / Infra** | 커밋 provenance + AWS 운영 전환 + repo hygiene |
| 🆕 | **LIMS** | HACCP·FSSC 22000·CCP·OOS·계측기 관리 |
| 🆕 | **EAM** | 설비 이력카드·견적 12년·CIP·냉동기 |
| 🆕 | **FEMS** | 전력 7년·물·살균·세척 사용량·이상 감지 |
| 🆕 | **Digital Twin** | 공장 CAD 154 + P&ID 43 + 라인 영상 overlay |
| 🆕 | **Safety / Env** | PSM 12요소·위험성평가·감사 증거팩 |
| 🆕 | **PLC / HMI** | KRONES PLC + KIRIN MMI + KUKA 로봇 심볼 인덱스 |
| 🆕 | **Jeseong Phase 1** | 제성팀 스마트팩토리 첫 단계 전용 화면 |

### 장기 (Phase 3+)
| 상태 | 과제 |
|:---:|------|
| 📋 | 사내 온프레미스 LLM + 공장 온톨로지 RAG |
| 📋 | VFD 인버터 다단 제어 + 자동밸브 피드백 |

---

## 📋 경력 요약

| 기간 | 소속 | 역할 | 핵심 성과 |
|------|------|------|----------|
| 2026.02 ~ 현재 | 보해양조 | **AI TF 팀장** | 스마트팩토리 E2E 설계·구축 |
| 2023.06 ~ 2026.01 | 보해양조 | 라인 운영 | 배합·충전·포장 운영, 설비 정비 |
| 2022.11 ~ 2023.05 | 오스템임플란트 | 영업 | **전국 신입사원 2위**, 성형외과 채널 개척 |
| 군 복무 | 공군 | 항공기 정비병 | 기체 정비, 영어 역량 → CQ 발탁 |

---

## ✅ 솔직하게

```diff
+ MES/APS/LIMS/EAM/IoT 설계·구현은 직접 수행했습니다.
+ 예산, 발주, 임원보고, 벤더 협상, 유관부서 조율도 직접 수행했습니다.
+ AI 에이전트를 도구로 활용하여 7주 만에 위 시스템을 E2E로 구축했습니다.
! PLC 직접 제어는 아직 완료하지 않았습니다 — 현재 S7-1200 학습 중입니다.
! Java/Spring/Oracle 실무 경험은 없습니다 — 도메인 + 아키텍처가 강점입니다.
```

---

<p align="center">
  <strong>허인회</strong> · 전남대학교 기계공학과 · 보해양조 제성공장
  <br>
  <em>제조 현장을 이해하고, 생산·품질·설비·계획을 하나의 실행 체계로 연결합니다.</em>
</p>

<p align="center">
  <sub>이 레포는 포트폴리오용 발췌본입니다. 운영 데이터와 인증 키는 보안상 비공개입니다.</sub>
</p>
