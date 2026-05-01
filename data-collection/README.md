# 한국천문연구원 OpenAPI 수집 스크립트

## 실행 전 준비
1. `.env` 파일에 아래 값을 설정합니다.
   - `KASI_API_KEY=발급받은_인증키`
2. 필요한 라이브러리를 자동 설치하려면 아래 명령어를 먼저 실행합니다.

```bash
python -m pip install -r requirements.txt
```

## 실행 명령어

```bash
python collect_solar_terms.py
```

```bash
python collect_daily_pillars.py
```

```bash
python collect_daily_pillars.py --retry-failed
```

```bash
python collect_daily_pillars.py --retry-from-file output\missing_dates_2014_2027.txt
```

## 일별 사주 스크립트 연도 범위 변경
- `collect_daily_pillars.py` 상단의 `START_YEAR`, `END_YEAR` 값을 원하는 연도로 수정하세요.
- 파일명은 `daily_pillars_{START_YEAR}_{END_YEAR}.csv` 형식으로 자동 생성됩니다.

## `collect_daily_pillars.py` 모드 안내
- 기본 모드: `python collect_daily_pillars.py`
  - `START_YEAR`~`END_YEAR` 전체 날짜를 순차 수집합니다.
  - 429 발생 시 `2s → 4s → 8s → 16s → 32s` 지수 백오프로 재시도합니다.
  - 응답 헤더에 `Retry-After`가 있으면 해당 초를 우선 사용합니다.
- 실패 재시도 모드: `python collect_daily_pillars.py --retry-failed`
  - `output/errors.log`에서 실패 날짜를 추출해 해당 날짜만 재호출합니다.
  - 결과를 기존 `output/daily_pillars_시작연도_종료연도.csv`에 병합합니다.
  - 날짜순 정렬 및 중복(`solar_date`) 제거를 수행합니다.
- 파일 지정 재시도 모드: `python collect_daily_pillars.py --retry-from-file <파일경로>`
  - 지정 파일에서 날짜 목록(한 줄 1개, `YYYY-MM-DD`)을 읽어 해당 날짜만 재호출합니다.
  - 결과를 기존 `output/daily_pillars_시작연도_종료연도.csv`에 병합합니다.
  - 날짜순 정렬 및 중복(`solar_date`) 제거를 수행합니다.
  - 예시: `python collect_daily_pillars.py --retry-from-file output\missing_dates_2014_2027.txt`

## 권장 실행 시간대
- 한국 시간 기준 새벽(01:00~07:00) 또는 저녁(20:00~24:00) 실행을 권장합니다.
- 트래픽이 많은 시간대에는 HTTP 429(호출 제한) 발생 가능성이 높습니다.

## 결과 파일
- CSV: `output/solar_terms_2000_2027.csv`
- CSV: `output/daily_pillars_시작연도_종료연도.csv`
- 오류 로그: `output/errors.log` (실패/빈 응답이 있을 때 기록)

## 변경 이력
- 2026-04-29
  - `collect_daily_pillars.py` 기본 요청 간격을 `0.3초`에서 `1.0초`로 변경.
  - HTTP 429 대응 지수 백오프(최대 5회) 및 `Retry-After` 우선 대기 적용.
  - `--retry-failed` 모드 추가(실패 날짜만 재수집 + CSV 병합/정렬/중복 제거).
  - 진행률 출력 형식을 `날짜 [현재/전체] 성공:x 실패:y 재시도중:z`로 개선.
  - 시작 시각/예상 완료 시각 출력 기능 추가.
- 2026-04-30
  - `--retry-from-file` 모드 추가(지정 파일 날짜만 재수집 + CSV 병합/정렬/중복 제거).
  - `errors.log` 파싱 시 로그 기록 시각이 아닌 실제 실패 대상 날짜를 우선 추출하도록 수정.
