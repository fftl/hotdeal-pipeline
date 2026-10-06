# hotdeal-pipeline

> 핫딜 커뮤니티 게시글을 즐겨 보는데, 매번 사이트를 직접 방문하는 대신 데이터를 수집하면 어떨까 하는 생각에서 시작한 프로젝트입니다.

여러 핫딜 커뮤니티의 RSS 피드를 주기적으로 수집해 데이터 웨어하우스에 원본 데이터를 축적하고, 계층형 변환을 거쳐 대시보드로 소비하기까지의 과정을 하나의 파이프라인으로 구성했습니다.

## 아키텍처

```
뽐뿌·어미새 RSS
      │  (매시간)
      ▼
Airflow DAG (Python, feedparser)
      │  append
      ▼
BigQuery raw ──dbt──▶ staging (view) ──dbt──▶ mart (table)
                                                  │
                                                  ▼
                                           Looker Studio
```

## 기술 스택

| 구분 | 사용 기술 |
|---|---|
| 언어 | Python, SQL |
| 오케스트레이션 | Apache Airflow 3.1.6 (LocalExecutor) |
| 웨어하우스 | Google BigQuery (`asia-northeast3`) |
| 변환·품질 테스트 | dbt (dbt-bigquery) |
| 시각화 | Looker Studio |
| 인프라 | Docker Compose, Oracle Cloud(OCI) ARM 인스턴스 |
| 환경 관리 | uv |

---

## 무엇을 (What)

- 뽐뿌·어미새 두 커뮤니티의 RSS 피드를 매시간 수집
- 수집 결과를 BigQuery `raw` 데이터셋에 가공 없이 append하여 이력 축적
- dbt `staging`에서 정제: 게시글 식별자 표준화, 게시시각 타입 변환, 중복 제거
- dbt `mart`에서 서울 시간 기준 분석 컬럼(날짜·시·요일) 생성 후 테이블로 저장
- dbt 테스트로 정제 결과 검증 (`unique`, `not_null`, `accepted_values`)
- Looker Studio 대시보드: 시간대별 분포 / 소스별 비중 / 일별 수집 추이

## 왜 (Why)

### 문제의식
알림 용도로 한 번 보고 지나치던 데이터를 이력으로 쌓고, 분석 가능한 형태로 만들면 어떤 정보를 얻을 수 있는지 확인하고자 했습니다. 핫딜 데이터는 수집 경로(RSS)가 명확하고 시계열 특성이 있어, 수집 → 적재 → 변환 → 시각화의 전 과정을 다루기에 적합한 소재라고 판단했습니다.

### 설계 원칙
- **raw는 원본 그대로 보존**: 수집 단계에서는 가공하지 않습니다. 정제 로직은 모두 dbt에서 관리하므로, 로직이 바뀌어도 재수집 없이 재처리할 수 있습니다.
- **레이어별 책임 분리**: `raw` / `staging` / `mart`를 BigQuery 데이터셋 단위로 분리해 각 단계의 역할을 명확히 했습니다.
- **재현성**: Airflow를 Docker Compose로 저장소 안에 포함해, 저장소를 clone하면 동일한 실행 환경을 구성할 수 있도록 했습니다.

## 어떻게 (How)

### 1. 수집·오케스트레이션 (Airflow)
- 수동으로 실행하던 수집 스크립트를 Airflow 3.x TaskFlow API 기반 DAG로 이관했습니다.
- DAG가 1개뿐인 규모에 맞춰 CeleryExecutor 대신 **LocalExecutor**를 사용하고, worker·redis·triggerer 등 불필요한 컨테이너를 제거했습니다. (postgres · apiserver · scheduler 중심 구성)
- BigQuery 서비스 계정 키는 읽기 전용 볼륨으로 마운트하고, Airflow 시크릿 값은 `.env`로 분리해 자격 증명을 코드와 분리했습니다.

### 2. 피드별 실패 감지
여러 피드 중 일부가 실패해도 DAG가 성공으로 표시되는 문제를 발견했습니다. 이를 해결하기 위해:
1. 피드별로 예외를 처리하고 수집 건수를 로깅
2. 수집에 성공한 데이터는 먼저 적재
3. 실패한 피드가 하나라도 있으면 예외를 발생시켜 태스크를 실패로 처리

"일부만 적재되었는데 성공으로 표시되는 상태"가 데이터 신뢰성 측면에서 가장 발견하기 어려운 오류라고 판단해, 경고 로그만 남기는 방식 대신 명시적 실패 처리를 선택했습니다.

### 3. 변환 (dbt)
| 레이어 | 형태 | 처리 내용 |
|---|---|---|
| `staging.stg_hotdeals` | view | 소스별 URL 패턴을 정규식으로 파싱해 `ppomppu_{id}`, `eomisae_{id}` 형태로 식별자 표준화 / RFC822 형식 게시시각 문자열을 `TIMESTAMP`로 변환(타임존 표기 형식 차이 대응) / `QUALIFY row_number()`로 게시글당 최신 수집분만 유지 |
| `mart.mart_hotdeals` | table | 서울 시간 기준 `deal_date`, `deal_hour`, `deal_weekday` 생성 / 대시보드 조회 성능을 위해 테이블로 저장 |

- staging은 항상 최신 상태를 반영하도록 view로, mart는 대시보드 조회용으로 table로 구분했습니다.
- dbt 기본 동작상 `+schema` 설정 시 데이터셋 이름에 접미사가 붙는 문제(`staging_staging`)가 있어, `generate_schema_name` 매크로를 재정의해 데이터셋 이름을 `staging`, `mart`로 맞췄습니다.

**품질 테스트**

| 대상 | 테스트 |
|---|---|
| `deal_id` | `unique`, `not_null` |
| `published_at` | `not_null` |
| `source` | `accepted_values` (ppomppu, eomisae) |

### 4. 시각화 (Looker Studio)
`mart.mart_hotdeals`를 연결해 다음 차트를 구성했습니다.
- 시간대별(0~23시) 게시 분포
- 소스별 비중
- 일별 수집 추이

## 배운 점 (Lessons)

- **실패 원인별 진단 방식의 차이**
  어미새는 `403`(User-Agent 차단)으로, 브라우저 UA 헤더를 추가해 해결했습니다. 반면 루리웹은 로컬에서는 정상이고 서버에서만 연결 타임아웃이 발생했습니다. `socket.create_connection`으로 TCP 연결 자체를 테스트해 클라우드 IP 대역 차단임을 확인했고, 코드로 해결할 수 없는 문제라고 판단해 수집 대상에서 제외했습니다. 우회 방법을 찾는 것보다 파이프라인 전체를 완성하는 것을 우선했습니다.

- **조용한 부분 실패의 위험성**
  다중 소스 수집에서는 일부 실패를 명시적으로 드러내는 것이 데이터 품질 관리의 출발점이라는 점을 확인했습니다. 또한 feedparser에는 기본 타임아웃이 없어, 응답이 없는 피드가 태스크 전체를 지연시킬 수 있다는 점도 알게 되었습니다.

- **타입은 소비 단계가 아니라 생산 단계에서 확정**
  Looker Studio에서 `deal_hour`가 날짜 타입으로 인식되는 문제가 있었습니다. 시각화 도구에서 매번 수정하는 대신, mart 모델에서 명시적으로 `int64`로 캐스팅해 근본적으로 해결했습니다.

- **dbt 디버깅 방법**
  dbt가 보고하는 에러 위치와 실제 원인이 다를 때, `target/`의 컴파일된 SQL을 BigQuery 콘솔에서 직접 실행해 원인을 특정했습니다. (예: CTE 이름 `source`가 동일 이름의 컬럼과 충돌하던 문제)

---

## 프로젝트 구조

```
hotdeal-pipeline/
├── dags/
│   └── hotdeal_dag.py            # RSS 수집 → BigQuery raw 적재
├── dbt/hotdeal/hotdeal/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_hotdeals.sql
│   │   │   └── schema.yml        # 품질 테스트 정의
│   │   └── mart/
│   │       └── mart_hotdeals.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
├── docker-compose.yaml           # Airflow (LocalExecutor)
├── auth/                         # 서비스 계정 키 (git 제외)
└── README.md
```

## 실행 방법

**사전 요구사항**: Docker, uv, BigQuery 서비스 계정 키

```bash
# 1. 저장소 클론
git clone <repository-url>
cd hotdeal-pipeline

# 2. 환경 변수 및 인증키 준비
#    - .env 작성 (Airflow 시크릿 값)
#    - auth/ 에 서비스 계정 키 배치

# 3. Airflow 실행
docker compose up -d

# 4. dbt 변환 및 테스트 (dbt 전용 Python 3.12 환경)
cd dbt/hotdeal/hotdeal
dbt run
dbt test
```

## 향후 개선 과제

- dbt 실행을 Airflow DAG에 통합해 수집부터 변환까지 단일 스케줄로 자동화
- 수집 소스 추가 (RSS 미제공 사이트는 수집 방식 별도 검토 필요)
