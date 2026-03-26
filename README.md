# Bohae Smart Factory — 제조DX 포트폴리오

> **보해양조 제성공장 스마트팩토리 구축 프로젝트** 코드 포트폴리오  
> PM/아키텍트: 허인회 · 전남대학교 기계공학과  
> 기간: 2023.06 ~ 현재

---

## 프로젝트 요약

보해양조 제성공장(주류 제조)의 생산계획, 생산실행, 품질, 설비 데이터를 하나의 운영 체계로 연결하는 **스마트팩토리 구축 프로젝트**입니다.

AI TF 팀장(3인)으로서 기술 설계와 구현을 주도했으며, 아래 시스템을 End-to-End로 구축했습니다.

### 핵심 수치

| 지표 | 값 |
|------|-----|
| IoT 카운터 검증 | **116,748개** 산업용 카운터와 정확히 일치 |
| MES/OPS 플랫폼 | **98개 페이지, 234개 API, 52개 서버 모듈** |
| APS 스케줄링 | **14개 라인 × 55일 → 60초** 내 산출 |
| 키오스크/전광판 | 외주 견적 **약 3억 원** → 자체 내재화 |
| 구축 기간 | **7주** (AI 에이전트 4종 병렬 운용) |

---

## 레포지토리 구조

```
bohae-smart-factory-portfolio/
├── aps-engine/                  # APS 생산 스케줄링 엔진 (Python)
│   ├── solver/                  # ★ CP-SAT 최적화 핵심 코드
│   │   ├── engine.py            # solve() + Big-M Lexicographic
│   │   ├── constraints.py       # 하드 제약조건
│   │   ├── objectives.py        # 6개 목적함수
│   │   ├── variables.py         # CP-SAT 변수 생성
│   │   ├── changeovers.py       # 전환시간 + Circuit
│   │   ├── breaks.py            # 휴게시간 제약
│   │   ├── staffing.py          # 인력 배정
│   │   ├── extract.py           # 솔루션 추출 + QC
│   │   ├── preprocess.py        # 수요 필터링, 세그먼트 분할
│   │   └── warm_start.py        # Warm start
│   ├── loaders/                 # SSOT 데이터 로더
│   ├── outputs/                 # Excel/DB 출력
│   ├── validators/              # Contract 검증
│   ├── postprocess/             # Rich 감사 보고서
│   ├── config.py                # Config 데이터클래스
│   └── main.py                  # CLI 진입점
│
├── mes-frontend/                # MES/OPS 프론트엔드 (Next.js 15 / TypeScript)
│   └── components/
│       ├── ops/                 # 운영 콘솔
│       ├── kiosk/               # 현장 키오스크
│       └── mes/                 # MES 레이아웃/쉘
│
├── docs/                        # 거버넌스 & 도메인 지식
│   ├── AGENTS.md                # AI 에이전트 코딩 가이드라인 (600줄)
│   └── jeseong_domain_knowledge.md  # 제성 공정 도메인 지식
│
└── evidence/                    # 현장 검증 사진
    ├── ESP32 and BUP 50S.jpeg   # ESP32 + 근접센서 설치
    ├── FX6Y Autonics 116748.jpeg # 산업용 카운터 116,748
    ├── Google Sheet 116748.jpeg  # 데이터 집계 116,748 일치
    └── Kiosk Retool.jpeg         # V1 키오스크 (Retool)
```

> ⚠️ **참고**: 이 레포는 포트폴리오용 발췌본입니다. SSOT 데이터, DB 스키마, 인증 키, 사내 문서 등은 보안상 제외되어 있습니다.

---

## 기술 스택

### Backend / APS

| 기술 | 용도 |
|------|------|
| Python 3.10+ | 백엔드, APS 엔진 |
| Google OR-Tools CP-SAT | 제약 만족 최적화 |
| FastAPI | REST API 서버 |
| PostgreSQL 16 | 운영 데이터베이스 |
| pandas / openpyxl | 데이터 처리 / Excel I/O |

### Frontend / MES

| 기술 | 용도 |
|------|------|
| Next.js 15 (App Router) | 풀스택 프레임워크 |
| TypeScript | 타입 안전 프론트엔드 |
| react-three-fiber | 3D 디지털 트윈 |
| Server-Sent Events | 실시간 키오스크 업데이트 |

### IoT / OT

| 기술 | 용도 |
|------|------|
| ESP32 (C++) | 엣지 디바이스 |
| MQTT / Mosquitto | 센서 데이터 전송 |
| IO-Link / Modbus TCP | 산업용 통신 (확장 중) |
| Coriolis / Magmeter | 유량 측정 (확장 중) |

### DevOps / 보안

| 기술 | 용도 |
|------|------|
| Docker | 컨테이너화 |
| GitHub Actions | CI/CD 4개 워크플로우 |
| OIDC/PKCE + AES-256-GCM | 인증/세션 암호화 |
| RBAC 5단계 | 인가 체계 |

---

## 아키텍처

```
센서 (근접/유량/온도/도수)
  ↓ 신호 탭 (비파괴)
ESP32 + ADC/PCNT
  ↓ MQTT (TLS)
Edge Server
  ↓
PostgreSQL 16
  ↓
FastAPI (APS + API)
  ↓
Next.js 15 (MES/OPS/키오스크/3D 트윈)
```

---

## APS 솔버 구조

```
SSOT Excel (19시트)
  ↓ ExcelLoader
DataBundle
  ↓ ContractValidator
  ↓ preprocess()
PreprocessResult
  ↓ create_variables()
  ↓ add_hard_constraints()
  ↓ build_objectives()
  ↓ _compute_lex_weights()   ← Big-M Lexicographic
  ↓ model.Minimize(obj_expr)
  ↓ solver.Solve(model)
  ↓ extract_result()
SolveResult → Excel 21시트 + DB write-back + 감사 보고서
```

### 목적함수 (6개, 사전식 우선순위)

```
minimize:
  1. UNSCHEDULED_COUNT   (미배정 최소화 — 최우선)
  2. TARDINESS_TOTAL     (납기 지연 최소화)
  3. EARLINESS_TOTAL     (조기 생산 최소화)
  4. NONPREFERRED_CNT    (비선호 라인 최소화)
  5. SETUP_TOTAL_MIN     (전환시간 최소화)
  6. BPM_SLOW_PEN        (저속 운전 최소화)
```

---

## 현장 검증 사진

### ESP32 + 근접센서 설치
![ESP32 설치](evidence/ESP32%20and%20BUP%2050S.jpeg)

### 산업용 카운터 116,748개 일치
![카운터 일치](evidence/FX6Y%20Autonics%20116748.jpeg)

---

## 경력 요약

| 기간 | 소속 | 역할 |
|------|------|------|
| 2026.02 ~ 현재 | 보해양조 | 스마트팩토리 AI TF 팀장 — 제조DX 설계·구축 |
| 2023.06 ~ 2026.01 | 보해양조 | 라인 운영, 설비 유지보수 (베어링/펌프 정비) |
| 2022.11 ~ 2023.05 | 오스템임플란트 | 영업 — 전국 신입 2위 |
| 군 복무 | 공군 | 항공기 정비병 → CQ(영어) |

---

## 연락처

- **이름**: 허인회
- **학력**: 전남대학교 기계공학과
- **이메일**: (지원서에 기재)

---

> 이 포트폴리오는 보해양조 제성공장 스마트팩토리 프로젝트의 기술적 성과를 보여주기 위한 발췌본입니다.  
> 전체 코드와 운영 데이터는 보안상 비공개입니다.
