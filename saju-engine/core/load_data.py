# -*- coding: utf-8 -*-
"""
박제된 만세력 CSV(절기·일별 기둥)를 메모리로 로드하고 무결성을 검증하는 모듈입니다.

경로는 `saju-engine` 폴더를 기준으로 상위 저장소 루트의 `data-collection/output` 을 참조합니다.
"""

from __future__ import annotations

import csv
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TypedDict


class DailyPillarRow(TypedDict, total=False):
    """일별 음양력·간지 CSV 한 행을 문자열 필드로 표현한 딕셔너리 타입입니다."""

    solar_date: str
    lun_year: str
    lun_month: str
    lun_day: str
    lun_leapmonth: str
    year_pillar: str
    day_pillar: str
    day_of_week: str
    julian_day: str


# 연도별 절기 시각 목록: (절기 시각, 절기 한글명, 태양 황경 또는 None)
SolarTermYearIndex = Dict[int, List[Tuple[datetime, str, Optional[float]]]]
# 날짜 문자열(YYYY-MM-DD) → 일별 기둥 행
DailyPillarsIndex = Dict[str, DailyPillarRow]


def _repository_root() -> Path:
    """이 파일이 위치한 `core` → `saju-engine` → 저장소 루트 경로를 반환합니다."""
    return Path(__file__).resolve().parent.parent.parent


def _data_collection_output_dir() -> Path:
    """
    박제 CSV가 저장된 `data-collection/output` 디렉터리 경로를 반환합니다.

    Returns:
        `solar_terms_2000_2027.csv` 등이 놓인 디렉터리.
    """
    return _repository_root() / "data-collection" / "output"


def _solar_terms_csv_path() -> Path:
    """2000~2027 절기 시각 CSV 파일 경로를 반환합니다."""
    return _data_collection_output_dir() / "solar_terms_2000_2027.csv"


def _daily_pillars_csv_paths() -> Tuple[Path, Path]:
    """2000~2013, 2014~2027 일별 기둥 CSV 두 경로를 순서대로 반환합니다."""
    out = _data_collection_output_dir()
    return out / "daily_pillars_2000_2013.csv", out / "daily_pillars_2014_2027.csv"


def _parse_solar_datetime(raw: str) -> datetime:
    """
    절기 CSV의 `datetime` 컬럼 문자열을 `datetime` 객체로 변환합니다.

    박제 원본에 `17:60`처럼 분 필드가 60인 행이 있어, 날짜에 `timedelta`를 더하는 방식으로
    파이썬이 시각을 자동 정규화하도록 합니다(예: 2019-01-20 17:60 → 2019-01-20 18:00).
    """
    text = (raw or "").strip()
    if not text:
        raise ValueError("datetime 값이 비어 있습니다.")

    # 예: 2000-01-06 10:01 또는 2000-01-06 10:01:02
    match = re.match(
        r"^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$",
        text,
    )
    if not match:
        raise ValueError(f"지원하지 않는 datetime 형식입니다: {raw!r}")

    date_part = match.group(1)
    hour = int(match.group(2))
    minute = int(match.group(3))
    second = int(match.group(4) or 0)

    base = datetime.strptime(date_part, "%Y-%m-%d")
    return base + timedelta(hours=hour, minutes=minute, seconds=second)


def _parse_sun_longitude(raw: str) -> Optional[float]:
    """태양 황경 문자열을 실수로 변환합니다. 빈 값이면 None 을 반환합니다."""
    text = (raw or "").strip()
    if not text:
        return None
    return float(text)


def load_solar_terms(
    csv_path: Optional[Path] = None,
) -> SolarTermYearIndex:
    """
    `solar_terms_2000_2027.csv`를 읽어 연도별 절기 시각 목록을 구성합니다.

    같은 연도 안에서는 절기 시각(`datetime` 컬럼) 순으로 정렬된 리스트를 돌려,
    이진 탐색 없이도 순차 참조하기 쉽게 합니다.

    Args:
        csv_path: 직접 경로를 지정할 때 사용합니다. None이면 기본 박제 경로를 사용합니다.

    Returns:
        `dict[연도] = [(절기 시각, 절기명, 태양 황경), ...]` 형태.

    Raises:
        FileNotFoundError: CSV 파일이 없을 때.
        ValueError: 필수 컬럼이 없거나 파싱에 실패할 때.
    """
    path = csv_path or _solar_terms_csv_path()
    if not path.is_file():
        raise FileNotFoundError(f"절기 CSV를 찾을 수 없습니다: {path}")

    by_year: Dict[int, List[Tuple[datetime, str, Optional[float]]]] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        required = {"year", "datetime", "dateName", "sunLongitude"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError(f"절기 CSV에 필요한 컬럼이 없습니다: {reader.fieldnames}")

        for row in reader:
            year = int(str(row["year"]).strip())
            dt = _parse_solar_datetime(str(row["datetime"]))
            name = str(row["dateName"]).strip()
            try:
                lon = _parse_sun_longitude(str(row.get("sunLongitude", "")))
            except ValueError as exc:
                raise ValueError(f"{year}행 sunLongitude 파싱 실패: {row}") from exc
            by_year.setdefault(year, []).append((dt, name, lon))

    for year, items in by_year.items():
        items.sort(key=lambda x: x[0])

    return by_year


def _read_daily_pillars_csv(path: Path) -> List[DailyPillarRow]:
    """단일 일별 기둥 CSV를 읽어 행 딕셔너리 리스트로 반환합니다."""
    if not path.is_file():
        raise FileNotFoundError(f"일별 기둥 CSV를 찾을 수 없습니다: {path}")

    rows: List[DailyPillarRow] = []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        if reader.fieldnames is None or "solar_date" not in reader.fieldnames:
            raise ValueError(f"일별 기둥 CSV에 solar_date 컬럼이 없습니다: {reader.fieldnames}")

        for row in reader:
            solar = str(row.get("solar_date", "")).strip()
            if not solar:
                continue
            # DictReader 값은 모두 문자열로 통일
            normalized: DailyPillarRow = {k: (v if v is not None else "") for k, v in row.items()}
            rows.append(normalized)

    return rows


def load_daily_pillars(
    paths: Optional[Tuple[Path, Path]] = None,
) -> DailyPillarsIndex:
    """
    `daily_pillars_2000_2013.csv`와 `daily_pillars_2014_2027.csv`를 모두 읽어 병합합니다.

    동일 `solar_date`가 두 파일에 있으면 **뒤쪽 파일(2014~2027)** 의 행이 우선합니다.
    병합 후 날짜 키를 오름차순으로 정렬한 딕셔너리를 반환합니다(파이썬 3.7+ 삽입 순서 유지).

    Args:
        paths: (2000~2013 경로, 2014~2027 경로) 튜플. None이면 기본 박제 경로를 사용합니다.

    Returns:
        `dict['YYYY-MM-DD'] = {컬럼명: 값, ...}`.

    Raises:
        FileNotFoundError: CSV가 없을 때.
        ValueError: 필수 컬럼이 없을 때.
    """
    p2000, p2014 = paths or _daily_pillars_csv_paths()
    merged: DailyPillarsIndex = {}

    # 먼저 2000~2013, 이어서 2014~2027을 덮어써 중복 날짜를 제거합니다.
    for path in (p2000, p2014):
        for row in _read_daily_pillars_csv(path):
            key = str(row["solar_date"]).strip()
            merged[key] = row

    return {k: merged[k] for k in sorted(merged.keys())}


def load_all() -> Dict[str, object]:
    """
    절기 데이터와 일별 기둥 데이터를 한꺼번에 로드합니다.

    Returns:
        `{"solar_terms": load_solar_terms(), "daily_pillars": load_daily_pillars()}` 형태의 dict.
    """
    return {
        "solar_terms": load_solar_terms(),
        "daily_pillars": load_daily_pillars(),
    }


def _expected_inclusive_days(start: date, end: date) -> int:
    """시작일·종료일을 포함한 연속 일수를 반환합니다."""
    return (end - start).days + 1


def _daterange_strings(start: date, end: date) -> List[str]:
    """시작~종료(포함) 모든 날짜를 'YYYY-MM-DD' 문자열 리스트로 반환합니다."""
    days = _expected_inclusive_days(start, end)
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


def validate_data(
    solar_terms: SolarTermYearIndex,
    daily_pillars: DailyPillarsIndex,
) -> bool:
    """
    박제 데이터 행 수·연도 범위·날짜 연속성을 검사합니다.

    검사 항목:
        - 절기: 2000~2027년 각 24행, 총 672행.
        - 일별 기둥: 2000-01-01~2027-12-31 매일 존재, 총 일수(윤년 반영)와 행 수 일치.
          기대 총일수는 10227일(365×28 + 윤일 7일)입니다.

    Args:
        solar_terms: `load_solar_terms()` 결과.
        daily_pillars: `load_daily_pillars()` 결과.

    Returns:
        모든 검사를 통과하면 True, 하나라도 실패하면 False.
    """
    start = date(2000, 1, 1)
    end = date(2027, 12, 31)
    expected_days = _expected_inclusive_days(start, end)
    expected_solar_rows = 24 * (end.year - start.year + 1)

    ok = True

    # --- solar_terms ---
    total_solar = sum(len(v) for v in solar_terms.values())
    if total_solar != expected_solar_rows:
        ok = False
        print(
            f"[실패] 절기 총 행 수 불일치: 기대 {expected_solar_rows}행, 실제 {total_solar}행"
        )

    missing_years: List[int] = []
    wrong_count_years: List[Tuple[int, int]] = []
    for y in range(start.year, end.year + 1):
        if y not in solar_terms:
            missing_years.append(y)
            ok = False
            continue
        cnt = len(solar_terms[y])
        if cnt != 24:
            wrong_count_years.append((y, cnt))
            ok = False

    if missing_years:
        print(f"[실패] 절기 데이터에 없는 연도: {missing_years}")
    if wrong_count_years:
        print(f"[실패] 연도별 절기 행 수가 24가 아닌 경우 (연도, 실제행수): {wrong_count_years}")

    # --- daily_pillars ---
    if len(daily_pillars) != expected_days:
        ok = False
        print(
            f"[실패] 일별 기둥 행 수 불일치: 기대 {expected_days}행, 실제 {len(daily_pillars)}행"
        )

    expected_dates = set(_daterange_strings(start, end))
    actual_dates = set(daily_pillars.keys())
    missing_dates = sorted(expected_dates - actual_dates)
    extra_dates = sorted(actual_dates - expected_dates)

    if missing_dates:
        ok = False
        print(f"[실패] 기간 내 누락된 날짜 {len(missing_dates)}건:")
        for d in missing_dates:
            print(d)
    if extra_dates:
        # 기간 밖 데이터가 있으면 알림(요구사항은 누락 위주이나 디버깅에 유용)
        print(f"[참고] 기간 밖 또는 예기치 않은 날짜 키 {len(extra_dates)}건(처음 20개만 표시):")
        for d in extra_dates[:20]:
            print(d)

    if ok:
        print("데이터 무결성 검증 통과")

    return ok


def _run_self_tests() -> None:
    """모듈 단독 실행 시 로드·검증을 수행하고 요약을 출력합니다."""
    print("=== load_data.py 자체 테스트 ===")
    try:
        bundle = load_all()
    except FileNotFoundError as exc:
        print(f"CSV를 찾을 수 없어 테스트를 건너뜁니다: {exc}")
        sys.exit(1)

    solar = bundle["solar_terms"]
    daily = bundle["daily_pillars"]
    assert isinstance(solar, dict) and isinstance(daily, dict)

    total_solar = sum(len(v) for v in solar.values())
    print(f"절기: 연도 수={len(solar)}, 총 행={total_solar}")
    print(f"일별 기둥: 총 행={len(daily)}, 첫날={next(iter(daily))}, 마지막={list(daily.keys())[-1]}")

    passed = validate_data(solar, daily)
    if not passed:
        sys.exit(1)

    # 샘플 검색(수동 검증·케이스 4 등)
    print("\n--- 샘플 검색 ---")

    # 1) 특정 양력일의 일주(일간·일지를 나타내는 day_pillar)
    d1 = "2024-05-15"
    row_may = daily[d1]
    print(f"1) {d1} 일주(day_pillar): {row_may.get('day_pillar', '')}")

    # 2) 해당 연도 CSV에 기록된 입춘(절기명이 '입춘'인 행) 시각 — 연중 1회가 일반적이나, 중복이 있으면 모두 표시
    solar_2024 = solar.get(2024, [])
    ipchun_rows = [(dt, name, lon) for dt, name, lon in solar_2024 if name == "입춘"]
    if not ipchun_rows:
        print("2) 2024년 입춘: 데이터 없음")
    else:
        for idx, (dt, _name, lon) in enumerate(ipchun_rows, start=1):
            lon_text = f", sunLongitude={lon}" if lon is not None else ""
            print(
                f"2) 2024년 입춘 시각 ({idx}/{len(ipchun_rows)}): "
                f"{dt.strftime('%Y-%m-%d %H:%M')}{lon_text}"
            )

    # 3) 케이스 4: 2012-11-07 일주가 임신(壬申)인지 확인
    d3 = "2012-11-07"
    row_nov = daily[d3]
    day_p = row_nov.get("day_pillar", "")
    print(f"3) {d3} 일주(day_pillar): {day_p}")
    expected_case4 = "임신(壬申)"
    if day_p != expected_case4:
        print(f"    [실패] 기대값 {expected_case4!r}, 실제값 {day_p!r}")
        sys.exit(1)
    print(f"    (케이스 4 기대값 {expected_case4!r} 일치)")

    print("=== 자체 테스트 종료 ===")


if __name__ == "__main__":
    _run_self_tests()
