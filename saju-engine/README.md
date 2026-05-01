# saju-engine

## 무엇을 하는가
사주 4기둥(연주/월주/일주/시주) 계산 핵심 로직을 담당합니다.

## 상태
TODO: 아직 작업 시작 전

## 모듈

- `core/tables.py`: 천간·지지·60갑자, 오호둔·오서둔, 절기→월지, 시각→시지(`get_hour_branch`) 등 사주 계산용 룩업 테이블과 `extract_stem` / `extract_branch` 보조 함수.
- `core/load_data.py`: `../data-collection/output` 박제 CSV(절기·일별 기둥)를 메모리로 로드(`load_solar_terms`, `load_daily_pillars`, `load_all`)하고 `validate_data`로 행 수·날짜 연속성을 검사합니다.
- `core/pillars.py`: 4기둥 계산 API를 둘 예정이며, 현재는 `calculate_day_pillar`(일주·자시 규칙 반영)만 제공합니다.

### 검증

```bash
python core/tables.py
```

```bash
python core/load_data.py
```

```bash
python core/pillars.py
```

## 의존
TODO: 작업 시작 시 작성
