# AGENTS.md — Bohae APS Engine 코딩 에이전트 지침서

> 이 파일은 GitHub Codex, Claude, Copilot 등 AI 코딩 에이전트가
> 이 저장소에서 작업할 때 반드시 따라야 하는 규칙과 컨텍스트입니다.

---

## 1. 프로젝트 개요

- **이름**: bohae-mes-aps-engine
- **목적**: 보해 음료 생산라인의 수요 배정/전환/인력 최적화 (APS)
- **언어**: Python 3.10+
- **핵심 의존성**: `ortools` (CP-SAT), `pandas`, `openpyxl`, `psycopg2-binary`
- **현재 버전**: v23 (태그: `v23.0.2`)

---

## 2. 아키텍처 (절대 원칙)

이 프로젝트는 **Ontology → Decision → Action** 3계층 구조를 따릅니다.

```
Ontology Layer    → SSOT Excel / PostgreSQL 로딩 + Contract 검증
Decision Layer    → CP-SAT 솔버 (Big-M Lexicographic 단일 패스)
Action Layer      → Excel 출력 (21시트) + DB write-back + Rich 감사 보고서
```

### SSOT 불변 원칙 (최우선 규칙)

**SSOT(Single Source of Truth) 데이터는 절대 코드로 수정하지 않는다.**

- SSOT 데이터 결함 발견 시 → `SSOT_ISSUE` 시트/리포트로만 보고
- 코드에서 SSOT 값을 하드코딩하거나 패치하는 것은 **금지**
- 우회 로직(workaround)도 금지 — 결함은 드러내고, 수정은 데이터 거버넌스로 처리

### 스레드 계약 SSOT (Specs)

Codex/에이전트 병렬 작업(worktree/스레드)은 기본적으로 독립적으로 진행될 수 있으므로,
아래 스펙을 **스레드 간 계약의 단일 진실(SSOT)** 로 취급합니다.

- `docs/canonical/specs/01_keys.md` (키/ID 정책)
- `docs/canonical/specs/02_ontology.md` (온톨로지: 객체/관계)
- `docs/canonical/specs/03_uns_events.md` (UNS/이벤트 스키마)
- `docs/canonical/specs/04_decision_writeback.md` (결정물/승인/write-back/감사)
- `docs/canonical/specs/05_security_markings.md` (보안 표식/전파/게이트)
- `docs/canonical/specs/06_lineage.md` (리니지 그래프/게이트)
- `docs/canonical/specs/07_regression_reconcile_v83.md` (회귀 수습 절차/발행 규칙)
- `docs/canonical/specs/08_v20_db_frontend_contract.md` (v20 API/프론트 계약)
- `docs/canonical/specs/14_feedback_contribution_contract.md` (피드백/기여도/랭킹 계약)
- `docs/canonical/specs/00_golden_path.md` (통합 골든패스 E2E)

스펙/계약을 바꾸는 변경은 코드보다 먼저 스펙을 갱신하고, PR에 근거(테스트/로그/증거)를 남깁니다.

### 문서 거버넌스 SSOT

문서성 산출물은 효력 레벨을 섞어 읽지 않습니다.

- 전역 문서 기준선: `docs/canonical/GOVERNANCE_DOCUMENT_SSOT.md`
- 전수 레지스트리: `docs/canonical/GOVERNANCE_FILE_REGISTRY.md`
- tracked 원장: `docs/canonical/GOVERNANCE_FILE_REGISTRY.tsv`

규약/정책/계약을 바꾸는 문서 작업은 먼저 위 기준선을 확인하고,
새 canonical 문서를 추가하기보다 기존 canonical 문서를 갱신합니다.
`exports/`, `proof_packs/`, 과거 handoff/bundle 폴더는 기본적으로 기준선이 아닙니다.

추가 가드레일:

- `docs/backend_corpus/00_INDEX.md`, `01_DOCUMENTS.md`, `02_CODE.md`, `03_INFRA_OPS.md`는 inventory/audit baseline이며 새 정책 원문이 아닙니다.
- `docs/codex/`는 hard zero-net-growth 존입니다. 새 문서를 추가하면 같은 가족의 기존 문서 하나 이상을 흡수, archive, `RETIRE_CANDIDATE` 처리합니다.
- `docs/handover/`, `docs/runbooks/`, `docs/dev/`는 active-reference 관리 존입니다. 새 문서는 허용하되 필수 헤더와 retire 계획이 있어야 합니다.
- 새 파생 문서는 `source_ssot`, `doc_role`, `status`, `owner`, `last_reviewed_on`, `retire_by`를 가집니다.
- `~/Downloads`, 메신저 사본, bundle 내부 복사본은 기준선이 될 수 없습니다. 필요 시 repo 안 정식 경로로 반입하고 효력 레벨을 부여합니다.
- `*.local_backup`, `*.bak`, `*.orig`, `*~` 같은 backup 파일은 tracked 문서로 유지하지 않습니다.
- `docs/canonical/specs/NN_*`는 번호 prefix를 중복 사용하지 않습니다.
- 문서 파일을 추가, 이동, 이름 변경했으면 `python scripts/governance/generate_registry.py`를 실행해 경고를 확인합니다.
- 로컬 훅/CI 문서 게이트: `python3 scripts/governance/check_document_policy.py --cached`, `python3 scripts/governance/check_document_policy.py --against-upstream`

### 운영형 전환 기준 문서

아래 문서는 현재 운영형 전환의 기준 문서로 취급합니다.

- `docs/canonical/BACKEND_OPERATIONAL_READINESS_AND_FEEDBACK_PLAN_20260307.md`
- `docs/canonical/EMPLOYEE_AUTH_UNIFICATION_PLAN_20260307.md`

인증/권한/피드백/랭킹/worker 관련 변경은 위 문서의 방향과 충돌하면 안 됩니다.

### 인증 / 식별 / 프로비저닝 원칙

현재 저장소에는 `ssot.staff_master.emp_no`가 존재하지만, 이것은 **인증원(authentication source)이 아니라 프로비저닝 소스**입니다.

- 로그인 ID는 `employee_no`
- 최종 인증 주체는 별도 IdP의 `auth_subject` (`sub`)
- 업무 권한은 `app_user`, `app_user_role`, plant/line scope가 담당
- `ssot.staff_master.emp_no`는 `app_user.employee_no` 생성/동기화의 근거로만 사용
- SSOT에 비밀번호/잠금/세션 상태를 저장하거나, SSOT를 로그인 DB처럼 사용하는 것은 금지
- `requested_by` body 필드와 `X-Bohae-User-Email` 헤더는 더 이상 인증 근거로 취급하지 않음
- FastAPI command/query actor는 bearer token + app DB lookup으로 확정해야 함
- `employee_no`는 leading zero를 보존해야 하므로 숫자형으로 캐스팅 금지

인증 관련 구현은 아래 순서를 따릅니다.

1. 스펙/계약 어휘(`employee_no`, `auth_subject`, `user_id`, `request_id`) 갱신
2. `app_user` identity 컬럼 추가
3. BFF session + bearer actor trust 전환
4. run actor closure / receipt / feedback score 도메인 반영

---

## 3. 디렉토리 구조 및 수정 범위

```
bohae-mes-aps-engine/
├── bohae_aps_v20/           ← 주 작업 대상
│   ├── main.py              # CLI 진입점
│   ├── config.py            # Config 데이터클래스
│   ├── loaders/             # Ontology 로더 (Excel + DB)
│   ├── solver/              # CP-SAT 최적화 핵심 (~2,900줄)
│   │   ├── engine.py        # solve() + Big-M lexicographic
│   │   ├── preprocess.py    # 수요 필터링, 세그먼트 분할
│   │   ├── variables.py     # CP-SAT 변수 생성
│   │   ├── constraints.py   # 하드 제약
│   │   ├── objectives.py    # 목적함수
│   │   ├── changeovers.py   # 전환시간 + Circuit
│   │   ├── breaks.py        # 휴게시간 제약
│   │   ├── staffing.py      # 인력 배정
│   │   ├── extract.py       # 솔루션 추출 + QC
│   │   └── warm_start.py    # Warm start
│   ├── outputs/             # Excel/DB 출력
│   ├── postprocess/         # Rich 감사 보고서
│   ├── validators/          # Contract 검증
│   ├── tools/               # 번들, 감사, 패치 도구
│   ├── agent/               # LLM 통합 (실험적, 건드리지 말 것)
│   ├── sql/                 # DB 스키마
│   └── utils/               # 헬퍼 함수
├── aps2_bootstrap_v2/       # SSOT QC 게이트 (WP-1)
├── SSOT/                    # SSOT Excel 원본 (읽기 전용)
└── solver_fix_instruction.md # compat symlink → docs/canonical/solver_fix_instruction.md
```

### 수정 가능 범위

| 범위 | 허용 |
|------|------|
| `bohae_aps_v20/solver/*` | O — 핵심 수정 대상 |
| `bohae_aps_v20/outputs/*` | O — 출력 로직 |
| `bohae_aps_v20/loaders/*` | O — 데이터 로딩 |
| `bohae_aps_v20/validators/*` | O — Contract 검증 |
| `bohae_aps_v20/tools/*` | O — 도구/번들 |
| `bohae_aps_v20/postprocess/*` | O — 리포트 |
| `bohae_aps_v20/config.py` | O — 설정 변경 (기본값 변경 시 주의) |
| `bohae_aps_v20/main.py` | O — CLI 변경 시 |
| `bohae_aps_v20/agent/*` | X — 실험적, 건드리지 말 것 |
| `SSOT/*.xlsx` | **X — 절대 수정 금지** |
| `aps2_bootstrap_v2/*` | 주의 — QC 로직 변경 시 기존 게이트 깨지지 않도록 |

---

## 4. 환경 설정 및 검증 명령어

### 의존성 설치

```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas openpyxl ortools psycopg2-binary
pip install -e ./aps2_bootstrap_v2
```

### 기본 실행 검증 (Excel 모드)

```bash
python -m bohae_aps_v20.main \
  --source excel \
  --ssot "SSOT/bohae_ontology (82).xlsx" \
  --scenario LIVE_BASE \
  --start 2026-01-01 \
  --end 2026-02-27 \
  --out "out/APS_TEST.xlsx" \
  --time_limit_sec 60 \
  --workers 8
```

### 번들 실행 검증 (수정 전/후 비교용)

```bash
python -m bohae_aps_v20.tools.run_bundle \
  --ssot "SSOT/bohae_ontology (82).xlsx" \
  --scenario LIVE_BASE \
  --start 2026-01-01 \
  --end 2026-02-27 \
  --time_limit_sec 60 \
  --workers 8
```

### 검증 기준

수정 후 반드시 아래를 확인:

1. **실행 성공**: 종료 코드 0 (또는 CONTRACT_FAIL이 아닌 정상 종료)
2. **출력 파일 생성**: `out/*.xlsx` 파일이 21개 시트를 포함
3. **회귀 없음**: 기존 baseline 대비 `UNSCHEDULED_COUNT`가 증가하지 않음
4. **TRACE 시트**: 수정 관련 지표가 정상 기록됨

---

## 5. 코딩 컨벤션

### Python 스타일

- `from __future__ import annotations` 모든 모듈 상단에 포함
- 타입 힌트 사용 (`Dict`, `List`, `Optional` 등 `typing`에서 임포트)
- 명시적 int 캐스팅: `int(value)`, `float(value)`, `bool(value)` — OR-Tools 호환성
- f-string 사용 (`.format()` 대신)
- snake_case (변수/함수), PascalCase (클래스)

### OR-Tools CP-SAT 규칙

- `cp_model.CpModel()` 인스턴스는 `model`로 명명
- `cp_model.CpSolver()` 인스턴스는 `solver`로 명명
- 변수 생성은 `variables.py`에서, 제약은 `constraints.py`에서
- 목적함수는 `objectives.py`에서 항목 계산, `engine.py`에서 Big-M 조합
- **int64 오버플로우 주의**: `MAX_WEIGHT = 9_000_000_000_000_000` 한계 준수
- 모든 수치는 `int`로 변환 후 모델에 전달 (float 금지)

### Config 규칙

- 새 설정 추가 시 `config.py`의 `Config` 데이터클래스에 기본값 포함
- CLI에서 override 가능하게 하려면 `main.py` argparse + `Config.with_overrides()` 연동
- 기본값 변경은 회귀 위험 — 변경 사유를 커밋 메시지에 명시

### 출력 규칙

- 새 출력 시트 추가 시 `excel_writer.py`의 `sheets` dict에 추가
- 키 이름은 `result` dict의 키와 매칭 (`snake_case_rows`)
- 시트 이름은 `UPPER_SNAKE_CASE`

---

## 6. 커밋 컨벤션

```
<type>(<scope>): <설명>
```

### type

| type | 용도 |
|------|------|
| `feat` | 새 기능 |
| `fix` | 버그 수정 |
| `refactor` | 동작 변경 없는 리팩토링 |
| `docs` | 문서 변경 |
| `data` | SSOT/데이터 관련 |
| `test` | 테스트 |
| `chore` | 빌드/설정/기타 |

### scope

| scope | 대상 |
|-------|------|
| `solver` | `bohae_aps_v20/solver/*` |
| `loader` | `bohae_aps_v20/loaders/*` |
| `output` | `bohae_aps_v20/outputs/*` |
| `config` | `bohae_aps_v20/config.py` |
| `cli` | `bohae_aps_v20/main.py` |
| `bundle` | `bohae_aps_v20/tools/run_bundle.py` |
| `qc` | `aps2_bootstrap_v2/*` |
| `db` | SQL 스키마, DB writer |
| `report` | `bohae_aps_v20/postprocess/*` |

### 예시

```
feat(solver): 전환시간 최적화에 format axis 추가
fix(solver): 동일 제품 전환시간 0 처리 누락 (BUG-003)
refactor(output): TRACE 시트를 flat key/value 구조로 변경
docs: README 아키텍처 다이어그램 추가
```

---

## 7. 핫픽스 절차

**모든 솔버 수정은 `docs/canonical/solver_fix_instruction.md`를 먼저 읽고 따른다.**

### 수정 워크플로우

1. **Baseline 생성**: 수정 전 `run_bundle` 실행 → `*_bundle.zip` 저장
2. **코드 수정**: `solver/`, `outputs/`, `extract.py` 등
3. **검증 실행**: 동일 파라미터로 `run_bundle` 재실행
4. **비교**: 수정 전/후 `TRACE`, `DATA_QUALITY`, `SOLVER_STATS` 비교
5. **커밋**: 변경 사유 + 증거 요약을 커밋 메시지에 포함

### 절대 하지 말 것

- SSOT Excel 파일 수정
- `config.py` 기본값을 검증 없이 변경
- `W_UNSCHEDULED` 등 페널티 가중치를 임의로 조정
- `time_limit_sec` 기본값 변경 (CLI override로만)
- 실패하는 테스트를 삭제하거나 무시

---

## 8. 현재 P0 이슈 (우선 작업 대상)

`docs/canonical/solver_fix_instruction.md`에 상세 기술된 P0 이슈:

| ID | 이슈 | 상태 |
|----|------|------|
| P0-0 | TRACE/DATA_QUALITY 계측 강화 | 진행중 |
| P0-1 | Lexicographic: FEASIBLE인데 unscheduled 고정 | Open |
| P0-2 | time_limit 60초 중 18초만 사용 | Open |
| P0-3 | SOLVER_STATS.solutions=0 (해가 있는데 0) | Open |
| P0-4 | PLAN_STAFF MISSING (인력 자격 제약 미반영) | Open |

**작업 우선순위**: P0-0 → P0-1 → P0-2 → P0-3 → P0-4

각 P0의 DoD(Definition of Done)는 `docs/canonical/solver_fix_instruction.md`에 증거 기준이 명시되어 있음.

---

## 9. 목적함수 구조 (수정 시 필독)

현재 `engine.py`의 단일 패스 Big-M Lexicographic:

```
minimize:
  UNSCHEDULED_COUNT * w1
+ TARDINESS_TOTAL   * w2
+ EARLINESS_TOTAL   * w3
+ NONPREFERRED_CNT  * w4
+ SETUP_TOTAL_MIN   * w5
+ BPM_SLOW_PEN      * w6
```

- `w1 >> w2 >> w3 >> w4 >> w5 >> w6` (Big-M 가중치로 보장)
- `_compute_lex_weights(bounds, order)`가 upper bound에서 자동 계산
- `lex_exact` 플래그로 사전식 보장 여부 추적
- **목적함수 순서 변경은 운영 영향이 매우 크므로 반드시 사전 승인 필요**

---

## 10. 데이터 흐름 요약

```
SSOT Excel (19시트)
  ↓ ExcelLoader / DBLoader
DataBundle (demands, capability_map, lines, shifts, calendar, staff, ...)
  ↓ ContractValidator
  ↓ preprocess()
PreprocessResult (filtered_demand_lines, segments, infeasible_set, ...)
  ↓ create_variables()
  ↓ add_hard_constraints()
  ↓ build_objectives()
  ↓ _compute_lex_weights()
  ↓ model.Minimize(obj_expr)
  ↓ solver.Solve(model)
  ↓ extract_result()
SolveResult (plan_rows, seg_rows, staff_rows, trace, ...)
  ↓ ExcelWriter.write()
  ↓ DBWriter.write()
  ↓ Rich Report
출력 완료
```

---

## 11. PR 작성 규칙

- PR 제목: 커밋 컨벤션과 동일 (`feat(solver): ...`)
- PR 본문에 포함할 것:
  - **변경 요약** (무엇을, 왜)
  - **검증 결과** (run_bundle 실행 결과, 수정 전/후 비교)
  - **관련 P0 이슈** (해당 시)
- Draft PR로 먼저 올리고, 검증 완료 후 Ready로 전환

---

## 11A. Proof Pack 제출 규칙

증거 제출은 아래 4종류로 구분합니다.

1. **문서 기준선 증명팩**
   - 정책/계약/어휘가 문서에 반영됐는지 증명
2. **구현 증명팩**
   - 코드/SQL/API가 실제로 바뀌었는지 증명
3. **실행 검증팩**
   - curl, SQL, 테스트, 로그로 동작을 증명
4. **비교 증명팩**
   - live frontend, frontend repo, backend/DB가 같은 기능을 기준으로 실제로 맞물리는지 비교 증명

### 기본 원칙

- **한 팩 = 한 질문** 원칙을 따른다.
- 사용자 요청이 별도 분할을 요구하지 않으면, **한 요청당 하나의 bundled proof pack**으로 제출한다.
- 팩 이름은 범위를 바로 알 수 있게 작성한다.
  - 예: `AUTH_FOUNDATION_PROOF_PACK`
  - 예: `RUN_ACTOR_CLOSURE_PROOF_PACK`
  - 예: `RECEIPT_UNIFICATION_PROOF_PACK`
  - 예: `FEEDBACK_SCORE_LEADERBOARD_PROOF_PACK`
- 일반 proof pack의 주장에는 반드시 `PASS / FAIL / PARTIAL` 판정이 있어야 한다.
- 모든 주장은 반드시 `file path + exact line` 근거를 가진다.
- auth 관련 팩은 반드시 `unauthorized / authorized` 비교 결과를 포함한다.
- DB가 진실원장인 주제(receipt, score, actor mapping)는 반드시 DB row 증거를 포함한다.
- 남은 구멍은 숨기지 말고 `OPEN_GAPS.md`에 명시한다.
- raw repo 전체를 넣지 말고, 변경 파일과 직접 의존 파일만 포함한다.
- compare proof pack은 반드시 아래 3개 증거를 함께 포함한다.
  - `Live UI evidence`
  - `Frontend source evidence`
  - `Backend / DB evidence`
- compare proof pack은 가능한 경우 `bohae.org` live URL, FE route file, BFF route, backend endpoint, DB table을 한 줄로 매핑한 `SCREEN_TO_API_MATRIX.md`를 포함한다.
- compare proof pack의 claim verdict 라벨은 아래 4개를 우선 사용한다.
  - `PASS`
  - `FRONTEND_ONLY`
  - `BACKEND_ONLY`
  - `DEPLOYMENT_DRIFT`
- live 접근 credential은 proof pack 안에 넣지 않는다.
  - `credential provided separately` 형태로만 적는다.

### 권장 폴더 구조

```text
PROOF_PACK_<scope>_<yyyymmdd>/
├── README_UPLOAD.md
├── CLAIMS_AND_VERDICTS.md
├── EVIDENCE_MATRIX.md
├── CHANGED_FILES.txt
├── PATCHES/
│   └── unified.diff
├── CODE_SNAPSHOTS/
├── TEST_OUTPUT/
├── CURL_OR_SQL_CHECKS/
└── OPEN_GAPS.md
```

```text
COMPARE_PROOF_PACK_<feature>_<yyyymmdd>/
├── README_UPLOAD.md
├── SCREEN_TO_API_MATRIX.md
├── LIVE_UI_EVIDENCE/
│   ├── request_response_samples.md
│   ├── console_notes.md
│   └── live_page_excerpt.md
├── FRONTEND_SOURCE_EVIDENCE.md
├── BACKEND_EXPECTATION.md
├── DB_EVIDENCE/
│   ├── schema_lines.md
│   └── select_results.md
├── PATCHES/
│   └── unified.diff
├── CLAIMS_AND_VERDICTS.md
└── OPEN_GAPS.md
```

### 필수 파일 규칙

- `CLAIMS_AND_VERDICTS.md`
  - 3~7개 claim 단위
  - 각 claim마다 `Claim`, `Pass Criteria`, `Verdict`, `Why`, `Remaining Gap`
- `EVIDENCE_MATRIX.md`
  - `Claim | Evidence Path | Lines | What it proves | Verdict`
- `CHANGED_FILES.txt`
  - 실제 변경 파일만 나열
- `PATCHES/unified.diff`
  - 가능하면 `git diff --unified=3`
- `TEST_OUTPUT/`
  - pytest, curl, SQL select, unauthorized/authorized 비교, receipt row 결과 등
- `OPEN_GAPS.md`
  - 미완료, 미검증, dev-only 예외, prod 차단사항 명시
- `SCREEN_TO_API_MATRIX.md`
  - `Screen | Live URL | FE Route File | BFF Route | Backend Endpoint | DB Table | Auth | Current Source of Truth | Verdict`
- `LIVE_UI_EVIDENCE/`
  - screenshot, HAR, curl request/response, localStorage/sessionStorage key notes, cookie name notes 중 가능한 증거를 포함
- `FRONTEND_SOURCE_EVIDENCE.md`
  - page/component/store/BFF 파일 경로와 줄번호 근거를 포함
- `BACKEND_EXPECTATION.md`
  - 해당 화면이 요구하는 backend endpoint, actor field, audit, fail mode를 정리

### 무엇을 보내면 안 되는가

- raw repo 전체
- `node_modules`, `.next`, 대형 캐시
- 줄번호 없는 요약문
- 테스트 없는 완료 선언
- 너무 넓은 범위를 한 팩에 모두 넣은 묶음

Proof pack 작성 템플릿은 `docs/canonical/templates/PROOF_PACK_TEMPLATE.md`를 우선 사용합니다.
Live FE와 repo/backend를 함께 비교하는 경우 `docs/canonical/templates/COMPARE_PROOF_PACK_TEMPLATE.md`를 우선 사용합니다.

---

## 12. 금지 사항 요약

1. **SSOT Excel 수정 금지**
2. **`agent/` 디렉토리 수정 금지** (실험적, 별도 관리)
3. **Config 기본값 무단 변경 금지**
4. **목적함수 우선순위 무단 변경 금지**
5. **`out/`, `.venv/`, `__pycache__/` 커밋 금지**
6. **`.env`, 비밀번호, API 키 커밋 금지**
7. **검증 없는 솔버 파라미터 변경 금지**

---

## 13. Git 가드레일 (에이전트 공통 필수)

에이전트는 작업 전/푸시 전 아래 가드레일을 반드시 통과해야 합니다.

### 작업 시작 전 (필수)

```bash
bash scripts/git_agent_preflight.sh
```

- 원격 대비 behind 상태면 작업/푸시 금지
- unresolved conflict 상태면 중단
- upstream 없는 브랜치 작업 금지

### 로컬 설치 (1회)

```bash
bash scripts/install_git_guardrails.sh
```

- `core.hooksPath=.githooks`
- `pull.ff=only`
- `fetch.prune=true`

### 대형 UI diff 보호

- `bohae_ops_web/src/components/ops/OpsConsole.tsx` staged 변경량이 3500줄 이상이면 pre-push에서 차단
- 의도된 대형 변경만 `ALLOW_MASSIVE_OPSCONSOLE_DIFF=1`로 명시적 override 허용
