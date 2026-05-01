# -*- coding: utf-8 -*-
"""
사주 4기둥(연·월·일·시) 계산 함수를 담는 모듈입니다.

- 연주: 입춘 시각 및 60갑자 순환(1984년 갑자 기준 세운연 ESY 매핑)
- 월주: 12절기(節) 시각 + 五虎遁
- 일주: 박제 ``daily_pillars`` 양력일 키의 ``day_pillar``
- 시주: 진태양시(현지 벽시계) + 오서둔; 시각 없으면 ``None``
- 통합: ``calculate_saju()`` 로 네 기둥 + 완성 여부
"""

from __future__ import annotations

import csv
import importlib.util
import io
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# `python core/pillars.py` 직접 실행과 `from core.pillars import ...` 패키지 import 모두 지원
try:
    from .load_data import (
        DailyPillarsIndex,
        SolarTermYearIndex,
        load_daily_pillars as _load_daily_pillars_fn,
        load_solar_terms as _load_solar_terms_fn,
    )
except ImportError:  # 스크립트로 실행 시 상대 import 불가
    _spec = importlib.util.spec_from_file_location(
        "_saju_core_load_data",
        Path(__file__).resolve().parent / "load_data.py",
    )
    if _spec is None or _spec.loader is None:
        raise ImportError("load_data.py를 불러올 수 없습니다.") from None
    _load_data_mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_load_data_mod)
    DailyPillarsIndex = _load_data_mod.DailyPillarsIndex  # type: ignore[no-redef]
    SolarTermYearIndex = _load_data_mod.SolarTermYearIndex  # type: ignore[no-redef]
    _load_daily_pillars_fn = _load_data_mod.load_daily_pillars
    _load_solar_terms_fn = _load_data_mod.load_solar_terms

try:
    from .tables import (
        EARTHLY_BRANCHES,
        FIVE_RAT_RULE,
        FIVE_TIGER_RULE,
        HEAVENLY_STEMS,
        SIXTY_GANJI,
        SOLAR_TERM_TO_MONTH_BRANCH,
        extract_stem,
        get_hour_branch,
    )
except ImportError:
    _t_spec = importlib.util.spec_from_file_location(
        "_saju_core_tables",
        Path(__file__).resolve().parent / "tables.py",
    )
    if _t_spec is None or _t_spec.loader is None:
        raise ImportError("tables.py를 불러올 수 없습니다.") from None
    _tables_mod = importlib.util.module_from_spec(_t_spec)
    _t_spec.loader.exec_module(_tables_mod)
    EARTHLY_BRANCHES = _tables_mod.EARTHLY_BRANCHES
    FIVE_RAT_RULE = _tables_mod.FIVE_RAT_RULE
    FIVE_TIGER_RULE = _tables_mod.FIVE_TIGER_RULE
    HEAVENLY_STEMS = _tables_mod.HEAVENLY_STEMS
    SIXTY_GANJI = _tables_mod.SIXTY_GANJI
    SOLAR_TERM_TO_MONTH_BRANCH = _tables_mod.SOLAR_TERM_TO_MONTH_BRANCH
    extract_stem = _tables_mod.extract_stem
    get_hour_branch = _tables_mod.get_hour_branch

try:
    from .solar_time import (
        calculate_apparent_solar_time,
        standard_utc_offset_east_minutes,
    )
except ImportError:
    _s_spec = importlib.util.spec_from_file_location(
        "_saju_core_solar_time",
        Path(__file__).resolve().parent / "solar_time.py",
    )
    if _s_spec is None or _s_spec.loader is None:
        raise ImportError("solar_time.py를 불러올 수 없습니다.") from None
    _solar_mod = importlib.util.module_from_spec(_s_spec)
    _s_spec.loader.exec_module(_solar_mod)
    calculate_apparent_solar_time = _solar_mod.calculate_apparent_solar_time
    standard_utc_offset_east_minutes = _solar_mod.standard_utc_offset_east_minutes

# 일별 기둥·절기 전체를 한 번만 읽어 두는 전역 캐시(프로세스 단위)
_daily_pillars_cache: Optional[DailyPillarsIndex] = None
_solar_terms_cache: Optional[SolarTermYearIndex] = None

# 박제 데이터로 일주를 조회할 수 있는 양력 연도 범위
_SUPPORTED_YEAR_MIN = 2000
_SUPPORTED_YEAR_MAX = 2027

# 세운연(입춘 간지년)을 60갑자 순번으로 맞출 때 사용하는 기준년·기준간지
# 입춘 시각이 지난 세운연 ESY = 1984 ⇒ 간지 갑자(甲子)로 두고 (ESY - 1984) mod 60 로 순환합니다.
_YEAR_PILLAR_REFERENCE_ESY = 1984

# 12절기(節) 이름 순서 — 세운연 안에서 시각순 배열 시 이 순서와 일치함(박제 데이터 기준 검증됨)
_TWELVE_JONG_ORDER: Tuple[str, ...] = tuple(SOLAR_TERM_TO_MONTH_BRANCH.keys())

# 한글 천간·지지 → 한자 표기(만세력 출력 형식용)
_HANJA_STEMS_STR = "甲乙丙丁戊己庚辛壬癸"
_HANJA_BRANCHES_STR = "子丑寅卯辰巳午未申酉戌亥"

# 국내 기본 출생지(원광·테스트 케이스와 동일한 서울 경도, KST 구역)
_DEFAULT_BIRTH_LONGITUDE = 126.978
_DEFAULT_BIRTH_TIMEZONE = "Asia/Seoul"


def _resolve_birth_location(
    birth_longitude: Optional[float], birth_timezone: Optional[str]
) -> Tuple[float, str]:
    """
    경도·IANA 시간대를 확정합니다.

    둘 다 생략되면 서울 기본값을 쓰고, 하나만 오면 예외를 던집니다.
    """
    if birth_longitude is None and birth_timezone is None:
        return float(_DEFAULT_BIRTH_LONGITUDE), _DEFAULT_BIRTH_TIMEZONE
    if birth_longitude is None or birth_timezone is None:
        raise ValueError(
            "birth_longitude 와 birth_timezone 은 둘 다 생략(서울 기본)하거나 둘 다 지정해야 합니다."
        )
    lon = float(birth_longitude)
    if not (-180.0 <= lon <= 180.0):
        raise ValueError(f"birth_longitude 는 -180~180 이어야 합니다. 입력: {birth_longitude}")
    tz = (birth_timezone or "").strip()
    if not tz:
        raise ValueError("birth_timezone 이 비어 있습니다.")
    return lon, tz


def _birth_instant_kst_naive(
    birth_date: str,
    birth_hour: Optional[int],
    birth_minute: Optional[int],
    birth_longitude: Optional[float],
    birth_timezone: Optional[str],
) -> datetime:
    """
    출생 **양력 일자 + 시계 시각**을 원광 호환 규칙으로 **KST naive datetime** 하나로 만듭니다.

    순서:
        1. ``calculate_apparent_solar_time`` — DST 없이 시계 그대로 + 경도·표준자오선 4분/도.
        2. 표준 ``utcoffset`` 만으로 로컬 → UTC → **UTC+9(KST)** .

    박제 절기·일주 CSV는 이 KST 시각축과 비교·조회합니다.
    """
    d = _parse_birth_date(birth_date)
    _validate_birth_year(d)
    hh, mm = _normalize_birth_clock(birth_hour, birth_minute)
    lon, tz = _resolve_birth_location(birth_longitude, birth_timezone)
    day_delta, ah, am = calculate_apparent_solar_time(hh, mm, lon, tz)
    local_cal = d + timedelta(days=day_delta)
    local_naive = datetime.combine(local_cal, time(ah, am))
    east_min = standard_utc_offset_east_minutes(tz)
    utc_naive = local_naive - timedelta(minutes=east_min)
    kst_naive = utc_naive + timedelta(hours=9)
    return kst_naive.replace(microsecond=0)


def _validate_kst_calendar_for_lookup(d: date) -> None:
    """KST로 옮긴 뒤의 양력일이 박제 CSV 범위에 들어오는지 검사합니다."""
    y = d.year
    if _SUPPORTED_YEAR_MIN <= y <= _SUPPORTED_YEAR_MAX:
        return
    if 1900 <= y <= 1999:
        raise NotImplementedError(
            "KST 기준 날짜가 1900~1999년으로 떨어져 아직 박제와 연동되지 않았습니다."
        )
    raise ValueError(
        f"KST 기준 날짜 {d.isoformat()} 의 연도는 박제 범위({_SUPPORTED_YEAR_MIN}~{_SUPPORTED_YEAR_MAX})에 없습니다."
    )


def _get_solar_terms() -> SolarTermYearIndex:
    """
    `load_solar_terms()` 결과를 전역 캐시에 담아 반환합니다.

    절기 진입 시각(분 단위) 비교에 사용합니다.
    """
    global _solar_terms_cache
    if _solar_terms_cache is None:
        _solar_terms_cache = _load_solar_terms_fn()
    return _solar_terms_cache


def _format_korean_ganji_two_char(kr_pair: str) -> str:
    """
    한글 천간·지지 두 글자(예: `'무자'`)를 만세력 출력 형식 `'무자(戊子)'` 로 만듭니다.
    """
    text = (kr_pair or "").strip()
    if len(text) != 2:
        raise ValueError(f"간지 두 글자가 아닙니다: {kr_pair!r}")
    stem_k, branch_k = text[0], text[1]
    if stem_k not in HEAVENLY_STEMS or branch_k not in EARTHLY_BRANCHES:
        raise ValueError(f"인식할 수 없는 천간·지지입니다: {kr_pair!r}")
    si = HEAVENLY_STEMS.index(stem_k)
    bi = EARTHLY_BRANCHES.index(branch_k)
    return f"{stem_k}{branch_k}({_HANJA_STEMS_STR[si]}{_HANJA_BRANCHES_STR[bi]})"


def _expand_csv_pillar_cell(cell: str) -> str:
    """`test_cases.csv`의 `무자` 형 셀을 `무자(戊子)` 형태로 통일합니다."""
    raw = (cell or "").strip()
    if not raw:
        return ""
    if "(" in raw:
        return raw
    return _format_korean_ganji_two_char(raw)


def _normalize_birth_clock(
    birth_hour: Optional[int], birth_minute: Optional[int]
) -> Tuple[int, int]:
    """
    연·월·일주 계산용 시계를 정합니다.

    ``birth_hour`` 또는 ``birth_minute`` 중 **하나라도** ``None``이면 **시각 없음**으로 보고
    자정 ``00:00``을 씁니다. (원광·한국 사주 사이트에서 흔히 쓰는 표준)

    시주(``calculate_hour_pillar``)는 이 함수를 쓰지 않고, 시·분 중 하나라도 ``None``이면
    ``None``을 반환합니다(임의 시각 금지).
    """
    # 시각 모르는 경우 자정 기준으로 통일 처리 (한국 사주 사이트 표준)
    if birth_hour is None or birth_minute is None:
        return 0, 0
    if not (0 <= birth_hour <= 23):
        raise ValueError(f"birth_hour는 0~23이어야 합니다. 입력: {birth_hour}")
    if not (0 <= birth_minute <= 59):
        raise ValueError(f"birth_minute는 0~59이어야 합니다. 입력: {birth_minute}")
    return birth_hour, birth_minute


def _ipchun_datetime_calendar_year(year: int, solar_terms: SolarTermYearIndex) -> datetime:
    """
    해당 **양력 연도 행 정렬번호 `year`(CSV의 year 컬럼)** 에 기록된 `입춘` 시각 중
    가장 이른 것을 반환합니다.

    같은 연도에 `입춘`이 두 번 잘못 박제된 해(예: 2000년)도 **가장 빠른 시각 하나**만
    진짜 입춘으로 취급합니다.
    """
    items = solar_terms.get(year)
    if not items:
        raise ValueError(f"절기 데이터에 연도 {year}년이 없습니다.")
    cands = [dt for dt, nm, _ in items if nm.strip() == "입춘"]
    if not cands:
        raise ValueError(f"{year}년도에 입춘(立春) 행을 찾을 수 없습니다.")
    return min(cands)


def _effective_solar_esy_year(birth_dt: datetime, solar_terms: SolarTermYearIndex) -> int:
    """
    출생 시각 기준 세운연(입춘이 지난 '간지 연도')의 양력 연번호를 반환합니다.

    - 그 해 입춘 시각 **미만**(``<``, 분까지 비교): 아직 새 세운이 아니므로 `양력연도 - 1`.
    - 입춘 시각 **이상**(``>=``, 절입 정각 포함): `양력연도` 번호 그대로를 세운연 번호로 씁니다.

    비교에는 박제 절기 `datetime`(naive)을 출생 시각과 직접 대조합니다.
    """
    calendar_year = birth_dt.year
    ip_this = _ipchun_datetime_calendar_year(calendar_year, solar_terms)
    # 의도: 17:27 입춘이면 17:26 까지만 전 세운.
    if birth_dt < ip_this:
        return calendar_year - 1
    return calendar_year


def _validate_esy_for_pillars(esy: int) -> None:
    """세운연 번호가 현재 박제·연산 지원 범위에 들어오는지 검사합니다."""
    if esy < 2000:
        if 1900 <= esy <= 1999:
            raise NotImplementedError(
                "1900~1999년 세운 구간은 아직 연·월주 박제와 연동되지 않았습니다."
            )
        raise ValueError(f"세운연 {esy}년은 지원 범위 밖입니다(2000년 이전).")
    if esy > _SUPPORTED_YEAR_MAX:
        raise ValueError(
            f"세운연 {esy}년은 박제 절기 데이터 범위({_SUPPORTED_YEAR_MAX}년)를 넘어섭니다."
        )


def _collect_twelve_jong_starts_in_esy(esy: int, solar_terms: SolarTermYearIndex) -> List[Tuple[datetime, str]]:
    """
    세운연 `[입춘(esy), 입춘(esy+1))` 반열구간 안에 들어오는 12절기(節)의
    진입 시각을 **정해진 월 순서대로** 반환합니다.

    한글 주석 의도:
    소한 등이 양력으로는 입춘보다 ''숫자만 보면'' 앞서 있어도, 세운연 구간 안에서만
    잘라 시계열을 만들면 축월~인월 순환이 자연스럽게 맞습니다.
    """
    ip0 = _ipchun_datetime_calendar_year(esy, solar_terms)
    ip1 = _ipchun_datetime_calendar_year(esy + 1, solar_terms)
    jong_set = set(_TWELVE_JONG_ORDER)
    best: Dict[str, datetime] = {}
    # ±1년까지 훑어 연말·연초 걸치는 절기를 포함합니다.
    for y in range(esy - 1, esy + 3):
        for dt, raw_name, _ in solar_terms.get(y, []):
            name = raw_name.strip()
            if name not in jong_set:
                continue
            if not (ip0 <= dt < ip1):
                continue
            if name not in best or dt < best[name]:
                best[name] = dt
    if set(best.keys()) != jong_set:
        raise ValueError(
            f"세운연 {esy}: 12절기(節) 시각 수집 실패 "
            f"(누락={sorted(jong_set - set(best.keys()))})."
        )
    ordered = [(best[nm], nm) for nm in _TWELVE_JONG_ORDER]
    return ordered


def _jong_name_for_datetime_in_esy(
    birth_dt: datetime, ordered_starts: List[Tuple[datetime, str]], esy_next_ipchun: datetime
) -> str:
    """
    `birth_dt`가 속한 절기 구간의 **앞쪽 절기 이름**(= 그 절기가 여는 월건)을 돌려줍니다.

    구간 규칙:
    - 절입 정각 포함: ``t0 <= birth_dt < t1``. 다음 절의 정각부터는 새 월.
    - 마지막 소한~(다음 세운연) 입춘 직전 까지는 축월.
    """
    for idx, (t0, nm) in enumerate(ordered_starts):
        if idx < len(ordered_starts) - 1:
            t1 = ordered_starts[idx + 1][0]
        else:
            t1 = esy_next_ipchun
        if t0 <= birth_dt < t1:
            return nm
    raise ValueError(
        f"출생 시각 {birth_dt} 을 세운연 절구간에 매칭하지 못했습니다."
    )


def calculate_year_pillar(
    birth_date: str,
    birth_hour: Optional[int] = None,
    birth_minute: Optional[int] = None,
    birth_longitude: Optional[float] = None,
    birth_timezone: Optional[str] = None,
) -> str:
    """
    양력 생년월일·시각·출생지로 **연주(年柱)** 를 구합니다. (**원광 호환 모드**)

    세운연 경계는 **입춘(立春) 정확 시각(분까지)** 기준입니다.
    출생 시각이 그 해 입춘 **이전**이면 전년 세운연의 연주를, **이후(정각 포함)** 이면
    당해 입춘이 속한 세운연의 연주를 씁니다.

    글로벌 출생: 시계 시각은 DST 보정 없이 받고, 경도·표준 자오선으로 진태양시 분 보정 후
    **표준 utcoffset 만**으로 KST로 바꾼 뒤, 박제 절기(KST)와 비교합니다.

    ``birth_hour`` 또는 ``birth_minute`` 가 ``None`` 이면 **시각 없음**으로 보고 **자정 00:00** 으로
    절기·세운연 비교에 씁니다(한국 사주 사이트 표준).
    ``birth_longitude`` / ``birth_timezone`` 이 모두 생략되면 **서울 기본값**을 씁니다.

    연간지 본체는 세운연 ESY에 대해 ``(ESY - 1984) mod 60`` 갑자 순환으로 얻으며,
    한국천문연 박제 절기와 정합을 맞춘 기준입니다.

    Args:
        birth_date: `'YYYY-MM-DD'` (출생지 양력 달력 날짜).
        birth_hour: ``0~23`` . ``None`` 이면 시각 없음(자정과 동일 처리).
        birth_minute: ``0~59`` . ``None`` 이면 시각 없음(자정과 동일 처리).
        birth_longitude: 출생지 경도(동경 +). 생략 시 서울 ``126.978``.
        birth_timezone: IANA 구역. 생략 시 ``Asia/Seoul``.

    Returns:
        `'무자(戊子)'` 형식 연주 문자열.

    Raises:
        ValueError: 형식·범위 오류, 절기 데이터 부족.
        NotImplementedError: 세운연이 1900~1999로 떨어지는 경우 등.
    """
    # 시각 모르는 경우 자정 기준으로 통일 처리 (한국 사주 사이트 표준) — _birth_instant_kst_naive 내부
    kst_dt = _birth_instant_kst_naive(
        birth_date, birth_hour, birth_minute, birth_longitude, birth_timezone
    )
    _validate_kst_calendar_for_lookup(kst_dt.date())

    solar_terms = _get_solar_terms()
    esy = _effective_solar_esy_year(kst_dt, solar_terms)
    _validate_esy_for_pillars(esy)

    idx = ((esy - _YEAR_PILLAR_REFERENCE_ESY) % 60 + 60) % 60
    return _format_korean_ganji_two_char(SIXTY_GANJI[idx])


def calculate_month_pillar(
    birth_date: str,
    birth_hour: Optional[int] = None,
    birth_minute: Optional[int] = None,
    year_pillar: Optional[str] = None,
    birth_longitude: Optional[float] = None,
    birth_timezone: Optional[str] = None,
) -> str:
    """
    양력 생년월일·시각·출생지로 **월주(月柱)** 를 구합니다. (**원광 호환 모드**)

    월지는 12절기(節)·입춘·경칩…·소한의 **진입 시각(분까지)** 으로만 나뉘며,
    중기(`우수`)·절후(`춘분`) 등은 쓰지 않습니다.

    월간은 `tables.FIVE_TIGER_RULE`(五虎遁)로 **연간(연주 천간)** 에 맞춰
    인월(寅월) 첫 천간을 정한 뒤, 인월부터 축월까지 천간이 열 순서대로 진행된다고 봅니다.

    비교 시각은 ``calculate_year_pillar`` 과 동일하게 **KST naive** 로 변환한 값입니다.

    Args:
        birth_date: `'YYYY-MM-DD'`.
        birth_hour: 시. ``None`` 이면 시각 없음(자정과 동일 처리).
        birth_minute: 분. ``None`` 이면 시각 없음(자정과 동일 처리).
        year_pillar: 이미 계산된 연주 문자열. `None`이면 내부에서 `calculate_year_pillar` 호출.
        birth_longitude: 출생지 경도. 생략 시 서울 기본.
        birth_timezone: IANA 구역. 생략 시 ``Asia/Seoul``.

    Returns:
        `'을축(乙丑)'` 형식 월주 문자열.
    """
    # 시각 모르는 경우 자정 기준으로 통일 처리 (한국 사주 사이트 표준) — _birth_instant_kst_naive 내부
    kst_dt = _birth_instant_kst_naive(
        birth_date, birth_hour, birth_minute, birth_longitude, birth_timezone
    )
    _validate_kst_calendar_for_lookup(kst_dt.date())

    solar_terms = _get_solar_terms()
    esy = _effective_solar_esy_year(kst_dt, solar_terms)
    _validate_esy_for_pillars(esy)

    yp = year_pillar if year_pillar is not None else calculate_year_pillar(
        birth_date,
        birth_hour,
        birth_minute,
        birth_longitude,
        birth_timezone,
    )
    year_stem_k = extract_stem(yp)

    tiger_stem_start = FIVE_TIGER_RULE[year_stem_k]
    tiger_stem_idx = HEAVENLY_STEMS.index(tiger_stem_start)

    starts = _collect_twelve_jong_starts_in_esy(esy, solar_terms)
    next_ipchun = _ipchun_datetime_calendar_year(esy + 1, solar_terms)
    jong_name = _jong_name_for_datetime_in_esy(kst_dt, starts, next_ipchun)
    branch_k = SOLAR_TERM_TO_MONTH_BRANCH[jong_name]
    month_ord = _TWELVE_JONG_ORDER.index(jong_name)
    stem_idx_m = (tiger_stem_idx + month_ord) % 10
    month_stem_k = HEAVENLY_STEMS[stem_idx_m]
    pair_k = month_stem_k + branch_k
    return _format_korean_ganji_two_char(pair_k)


def _get_daily_pillars() -> DailyPillarsIndex:
    """
    `load_daily_pillars()` 결과를 전역 캐시에 담아 반환합니다.

    동일 프로세스에서 반복 호출 시 CSV를 다시 읽지 않습니다.

    Returns:
        `solar_date`(YYYY-MM-DD) 키 → 행 딕셔너리.
    """
    global _daily_pillars_cache
    if _daily_pillars_cache is None:
        _daily_pillars_cache = _load_daily_pillars_fn()
    return _daily_pillars_cache


def _parse_birth_date(birth_date: str) -> date:
    """`YYYY-MM-DD` 문자열을 `date`로 변환합니다. 형식이 잘못되면 예외를 던집니다."""
    text = (birth_date or "").strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"birth_date는 'YYYY-MM-DD' 형식이어야 합니다. 입력값: {birth_date!r}"
        ) from exc


def _validate_birth_year(d: date) -> None:
    """
    출생일 연도가 현재 박제 데이터에서 지원하는지 검사합니다.

    - 1900~1999년: 추후 박제·로직 추가 예정이므로 `NotImplementedError`.
    - 2000~2027년: 허용.
    - 그 외: `ValueError`로 명시합니다.
    """
    y = d.year
    if _SUPPORTED_YEAR_MIN <= y <= _SUPPORTED_YEAR_MAX:
        return
    if 1900 <= y <= 1999:
        raise NotImplementedError(
            "1900~1999년 출생자 일주 조회는 아직 지원하지 않습니다. "
            "천문연 박제 확장 또는 별도 만세력 연동 후 추가 예정입니다."
        )
    raise ValueError(
        f"birth_date의 연도 {y}는 박제 데이터 범위({_SUPPORTED_YEAR_MIN}~{_SUPPORTED_YEAR_MAX})에 없습니다."
    )


def _lookup_day_pillar_string(lookup_date: date, pillars: DailyPillarsIndex) -> str:
    """주어진 양력일 키로 `day_pillar` 필드를 읽어 반환합니다. 없으면 예외."""
    key = lookup_date.isoformat()
    if key not in pillars:
        raise ValueError(
            f"날짜 {key}에 대한 일별 데이터가 없습니다. "
            f"박제 범위({_SUPPORTED_YEAR_MIN}~{_SUPPORTED_YEAR_MAX})인지 확인하세요."
        )
    row = pillars[key]
    pillar = (row.get("day_pillar") or "").strip()
    if not pillar:
        raise ValueError(f"날짜 {key} 행에 day_pillar 값이 비어 있습니다.")
    return pillar


def calculate_day_pillar(
    birth_date: str,
    birth_hour: Optional[int] = None,
    birth_minute: Optional[int] = None,
    birth_longitude: Optional[float] = None,
    birth_timezone: Optional[str] = None,
) -> str:
    """
    양력 생년월일·시각·출생지로 **일주(日柱)** 문자열을 구합니다. (**원광 호환 모드**)

    박제 ``daily_pillars`` 의 ``solar_date`` 키는 **한국 표준시(KST) 양력 달력**입니다.

    **연·월주**는 진태양시·표준시 오프셋으로 KST 시각축에 올린 뒤 절기와 맞춥니다.
    **일주**는 원광 글로벌 표기와 같이, 사용자가 입력한 **출생지 양력 표기일**
    ``birth_date`` 행의 ``day_pillar`` 를 그대로 씁니다(시·분·경도는 일간지를 바꾸지 않음).

    ``birth_longitude`` / ``birth_timezone`` 은 형식 검증·API 일관성을 위해 받으며,
    일주 조회 키에는 쓰이지 않습니다.

    Args:
        birth_date: 출생지 양력 `'YYYY-MM-DD'` (일주 룩업 키).
        birth_hour: 시(0~23). ``None`` 이면 시각 없음(연·월·일의 시계 확정만 자정으로 통일).
        birth_minute: 분(0~59). ``None`` 이면 동일.
        birth_longitude: 출생지 경도. 생략 시 서울 기본.
        birth_timezone: IANA 구역. 생략 시 ``Asia/Seoul``.

    Returns:
        일주 한글(한자) 문자열(예: `'임신(壬申)'`).

    Raises:
        ValueError: 날짜 형식 오류, 시·분 범위 오류, 박제 범위 밖(2000~2027 외) 등.
        NotImplementedError: 1900~1999년 출생 등 미구현 구간.
    """
    d = _parse_birth_date(birth_date)
    _validate_birth_year(d)
    # 시각 모르는 경우 자정 기준으로 통일 처리 (한국 사주 사이트 표준) — 연·월과 동일 규칙으로 시계만 확정
    _normalize_birth_clock(birth_hour, birth_minute)
    _resolve_birth_location(birth_longitude, birth_timezone)

    pillars = _get_daily_pillars()
    return _lookup_day_pillar_string(d, pillars)


def calculate_hour_pillar(
    birth_date: str,
    birth_hour: Optional[int],
    birth_minute: Optional[int],
    day_pillar: str,
    birth_longitude: Optional[float] = None,
    birth_timezone: Optional[str] = None,
) -> Optional[str]:
    """
    시주(時柱)를 구합니다.

    ``birth_hour`` 또는 ``birth_minute`` 중 **하나라도** ``None``이면 **시각 없음**으로 보고
    ``None``을 돌려줍니다(임의 시각으로 시지를 채우지 않음).

    시각이 모두 있으면:
        1. ``calculate_apparent_solar_time`` 으로 진태양시(경도·표준자오선 4분/도)만 보정한
           **출생지 벽시계** 날짜·시·분을 구합니다.
        2. 그 시·분으로 ``get_hour_branch`` 에서 시지(時支)를 정합니다.
        3. ``day_pillar`` 에서 일간(日干)을 ``extract_stem`` 으로 뽑습니다.
           (야자 23:30~23:59·조자 00:00~01:29는 ``calculate_day_pillar`` 가 고른 일주 행과
           원광이 맞도록 이미 정해져 있으므로, 시주는 그 일간을 그대로 씁니다.)
        4. ``FIVE_RAT_RULE``(五鼠遁)로 자시(子時) 구간의 시작 천간을 정한 뒤,
           시지 순서만큼 천간을 열(10) 순환하여 시간(時干)을 맞춥니다.
        5. ``시간+시지`` 를 ``_format_korean_ganji_two_char`` 형식으로 반환합니다.

    Args:
        birth_date: ``'YYYY-MM-DD'`` 출생지 양력 표기(진태양 보정으로 날짜가 하루 밀릴 수 있음).
        birth_hour: 시(0~23).
        birth_minute: 분(0~59).
        day_pillar: 일주 문자열(예: ``'임신(壬申)'``).
        birth_longitude: 출생지 경도. 생략 시 서울 기본.
        birth_timezone: IANA 구역. 생략 시 ``Asia/Seoul``.

    Returns:
        예: ``'갑술(甲戌)'`` . 시각 없음이면 ``None`` .
    """
    if birth_hour is None or birth_minute is None:
        return None
    if not (0 <= birth_hour <= 23):
        raise ValueError(f"birth_hour는 0~23이어야 합니다. 입력: {birth_hour}")
    if not (0 <= birth_minute <= 59):
        raise ValueError(f"birth_minute는 0~59이어야 합니다. 입력: {birth_minute}")

    d = _parse_birth_date(birth_date)
    _validate_birth_year(d)
    lon, tz = _resolve_birth_location(birth_longitude, birth_timezone)

    # 시지는 연·월의 KST 절기축이 아니라, 진태양 보정만 한 **현지 벽시계**로 둡니다(원광 시주 검증과 동일).
    day_delta, ah, am = calculate_apparent_solar_time(birth_hour, birth_minute, lon, tz)
    local_cal = d + timedelta(days=day_delta)
    local_naive = datetime.combine(local_cal, time(ah, am))

    branch_k = get_hour_branch(local_naive.hour, local_naive.minute)
    day_stem_k = extract_stem(day_pillar)
    zi_start_stem = FIVE_RAT_RULE[day_stem_k]
    zi_idx = HEAVENLY_STEMS.index(zi_start_stem)
    br_idx = EARTHLY_BRANCHES.index(branch_k)
    hour_stem_k = HEAVENLY_STEMS[(zi_idx + br_idx) % 10]
    return _format_korean_ganji_two_char(hour_stem_k + branch_k)


def calculate_saju(
    birth_date: str,
    birth_hour: Optional[int],
    birth_minute: Optional[int],
    birth_longitude: Optional[float] = None,
    birth_timezone: Optional[str] = None,
) -> Dict[str, Any]:
    """
    연·월·일·시 네 기둥을 한 번에 계산합니다.

    Args:
        birth_date: ``'YYYY-MM-DD'`` 출생지 양력.
        birth_hour: 시. ``None``이면 시각 없음(연·월·일은 자정 기준, 시주는 ``None``).
        birth_minute: 분.
        birth_longitude: 출생지 경도. 생략 시 서울.
        birth_timezone: IANA 시간대. 생략 시 ``Asia/Seoul``.

    Returns:
        다음 키를 가진 ``dict`` 입니다.

        - ``year_pillar`` (``str``): 연주, 예 ``'무자(戊子)'`` .
        - ``month_pillar`` (``str``): 월주.
        - ``day_pillar`` (``str``): 일주.
        - ``hour_pillar`` (``Optional[str]``): 시주. 시각 없으면 ``None`` .
        - ``is_complete`` (``bool``): ``hour_pillar`` 가 ``None`` 이 아니면 ``True`` , 아니면 ``False`` .
    """
    lon = birth_longitude
    tz = birth_timezone
    year_pillar = calculate_year_pillar(birth_date, birth_hour, birth_minute, lon, tz)
    month_pillar = calculate_month_pillar(
        birth_date, birth_hour, birth_minute, year_pillar, lon, tz
    )
    day_pillar = calculate_day_pillar(birth_date, birth_hour, birth_minute, lon, tz)
    hour_pillar = calculate_hour_pillar(
        birth_date, birth_hour, birth_minute, day_pillar, lon, tz
    )
    return {
        "year_pillar": year_pillar,
        "month_pillar": month_pillar,
        "day_pillar": day_pillar,
        "hour_pillar": hour_pillar,
        "is_complete": hour_pillar is not None,
    }


def _run_self_tests() -> None:
    """
    ``test_cases.csv`` 9건으로 연·월·일·시주 및 ``calculate_saju`` 통합을 검증합니다.

    ``answer_year`` / ``answer_month`` / ``answer_day`` / ``answer_hour`` 셀을
    ``무자`` → ``무자(戊子)`` 형태로 바꿔 비교합니다. 시각 없는 행은 ``answer_hour`` 가 비어
    ``hour_pillar=None`` 이어야 합니다.
    """
    print("=== pillars.py 자체 테스트 ===")

    tests_path = Path(__file__).resolve().parent.parent / "test_cases.csv"
    if not tests_path.is_file():
        print(f"test_cases.csv를 찾을 수 없습니다: {tests_path}")
        sys.exit(1)

    rows: List[
        Tuple[
            int,
            str,
            str,
            Optional[int],
            Optional[int],
            float,
            str,
            str,
            str,
            str,
            str,
        ]
    ] = []
    raw_bytes = tests_path.read_bytes()
    decoded: Optional[str] = None
    for enc in ("utf-8-sig", "cp949", "utf-8"):
        try:
            decoded = raw_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if decoded is None:
        raise ValueError("test_cases.csv를 utf-8/cp949로 디코딩할 수 없습니다.")

    reader = csv.DictReader(io.StringIO(decoded, newline=""))
    for row in reader:
        name = (row.get("name") or "").strip()
        solar_date = (row.get("solar_date") or "").strip()
        time_raw = (row.get("solar_time") or "").strip()
        if not name or not solar_date:
            continue
        try:
            case_id = int((row.get("case_id") or "0").strip())
        except ValueError:
            case_id = 0
        hour: Optional[int] = None
        minute: Optional[int] = None
        if time_raw:
            parts = time_raw.split(":")
            if len(parts) != 2:
                print(f"[오류] solar_time 형식: {time_raw!r} (케이스 {name})")
                sys.exit(1)
            hour = int(parts[0])
            minute = int(parts[1])

        lon_raw = (row.get("birth_longitude") or "").strip()
        tz_raw = (row.get("birth_timezone") or "").strip()
        if not lon_raw or not tz_raw:
            lon_f, tz_s = _DEFAULT_BIRTH_LONGITUDE, _DEFAULT_BIRTH_TIMEZONE
        else:
            lon_f = float(lon_raw)
            tz_s = tz_raw

        ay_raw = (row.get("answer_year") or "").strip()
        ay = _expand_csv_pillar_cell(ay_raw)
        am = _expand_csv_pillar_cell(row.get("answer_month", ""))
        ad = _expand_csv_pillar_cell(row.get("answer_day", ""))
        ah_raw = (row.get("answer_hour") or "").strip()
        ah = _expand_csv_pillar_cell(ah_raw) if ah_raw else ""
        if not ay or not am or not ad:
            print(f"[오류] answer_year/month/day 누락 — 케이스 {name}")
            sys.exit(1)
        rows.append((case_id, name, solar_date, hour, minute, lon_f, tz_s, ay, am, ad, ah))

    rows.sort(key=lambda r: (r[0], r[1]))

    ok_y = ok_m = ok_d = ok_h = 0
    total = len(rows)
    all_pass = True
    for case_id, name, solar_date, hour, minute, lon_f, tz_s, exp_y, exp_m, exp_d, exp_h in rows:
        got_y = calculate_year_pillar(
            solar_date, hour, minute, lon_f, tz_s
        )
        got_m = calculate_month_pillar(
            solar_date, hour, minute, None, lon_f, tz_s
        )
        got_d = calculate_day_pillar(solar_date, hour, minute, lon_f, tz_s)
        got_h = calculate_hour_pillar(
            solar_date, hour, minute, got_d, lon_f, tz_s
        )
        my = got_y == exp_y
        mm = got_m == exp_m
        md = got_d == exp_d
        if exp_h:
            mh = got_h == exp_h
        else:
            mh = got_h is None
        if my:
            ok_y += 1
        if mm:
            ok_m += 1
        if md:
            ok_d += 1
        if mh:
            ok_h += 1
        case_ok = my and mm and md and mh
        if not case_ok:
            all_pass = False

        print(f"케이스 {name} (case_id={case_id}): ")
        print(f"  연주: 함수={got_y}, 기대={exp_y}, 일치={'✅' if my else '❌'}")
        print(f"  월주: 함수={got_m}, 기대={exp_m}, 일치={'✅' if mm else '❌'}")
        print(f"  일주: 함수={got_d}, 기대={exp_d}, 일치={'✅' if md else '❌'}")
        exp_h_disp = exp_h if exp_h else "None"
        got_h_disp = got_h if got_h is not None else "None"
        print(f"  시주: 함수={got_h_disp}, 기대={exp_h_disp}, 일치={'✅' if mh else '❌'}")

        if not case_ok:
            t_disp = (
                f"{hour:02d}:{minute:02d}"
                if hour is not None and minute is not None
                else "시각없음"
            )
            print(
                f"  >>> 불일치 강조: 케이스 {name} "
                f"(입력 {solar_date} 시각={t_disp})\n"
            )

    print(
        f"\n=== 통계: 연주 {ok_y}/{total}, 월주 {ok_m}/{total}, 일주 {ok_d}/{total}, "
        f"시주 {ok_h}/{total} (총 {total}케이스) ==="
    )

    print("\n=== calculate_saju() 통합 (9건) ===")
    for case_id, name, solar_date, hour, minute, lon_f, tz_s, exp_y, exp_m, exp_d, exp_h in rows:
        combo = calculate_saju(solar_date, hour, minute, lon_f, tz_s)
        y_ok = combo["year_pillar"] == exp_y
        m_ok = combo["month_pillar"] == exp_m
        d_ok = combo["day_pillar"] == exp_d
        if exp_h:
            h_ok = combo["hour_pillar"] == exp_h
            want_complete = True
        else:
            h_ok = combo["hour_pillar"] is None
            want_complete = False
        complete_ok = combo["is_complete"] is want_complete
        saju_ok = y_ok and m_ok and d_ok and h_ok and complete_ok
        if not saju_ok:
            all_pass = False
        print(
            f"  [{name}] year={combo['year_pillar']} month={combo['month_pillar']} "
            f"day={combo['day_pillar']} hour={combo['hour_pillar']} "
            f"is_complete={combo['is_complete']} → {'✅' if saju_ok else '❌'}"
        )

    if all_pass:
        print("\n=== 모든 케이스 통과 (기둥별 + calculate_saju) ===")
    else:
        sys.exit(1)


if __name__ == "__main__":
    _run_self_tests()
