# -*- coding: utf-8 -*-
"""
원광만세력 호환 모드: 사용자가 입력한 **시계 시각**을 그대로 두고,
출생지 경도와 해당 시간대의 **표준 자오선** 차이만큼 분 단위로 보정(진태양시 근사)합니다.

- **DST(일광절약시)는 인식하지 않습니다.** IANA 구역의 ``1월 중순`` 앵커 시각으로
  ``utcoffset`` 을 읽어 **표준시** 오프셋만 쓰고, 여름·겨울 규칙을 출생일에 맞추어 바꾸지 않습니다.
- 박제 절기·일주 데이터는 **KST(naive)** 기준이므로, ``pillars`` 모듈에서 본 보정 후
  다시 표준 오프셋만으로 KST로 옮깁니다.
"""

from __future__ import annotations

import sys
from datetime import datetime
from typing import Dict, Tuple

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError as exc:  # Python < 3.9 (이론상)
    raise ImportError("zoneinfo(표준 라이브러리)가 필요합니다. Python 3.9 이상을 사용하세요.") from exc


# Windows 등 ``tzdata`` 미설치 시 ``ZoneInfo`` 가 실패할 수 있어, 자주 쓰는 구역의 **표준시** 분 오프셋을 박제합니다.
# (DST 무시 — 여름철 PDT 등은 적용하지 않음)
_FALLBACK_STANDARD_EAST_MINUTES: Dict[str, int] = {
    "Asia/Seoul": 540,
    "Asia/Tokyo": 540,
    "America/Los_Angeles": -480,
    "America/New_York": -300,
    "America/Chicago": -360,
    "America/Denver": -420,
    "Europe/London": 0,
    "UTC": 0,
    "Etc/UTC": 0,
}


def standard_utc_offset_east_minutes(timezone_str: str) -> int:
    """
    IANA 시간대 문자열에 대해 **DST를 무시한 표준시** 기준,
    ``UTC + 반환값(분) = 해당 구역의 표준 시각`` 이 되도록 분 단위 오프셋을 반환합니다.

    구현: 매년 **1월 15일 12:00** 로컬 시각을 잡고 ``utcoffset`` 을 읽습니다.
    북반구에서 대부분의 구역이 표준시를 쓰는 시기이며, 원광 호환 모드에서
    ``DST 시기 인식하지 않음`` 요구에 맞춘 고정 앵커입니다.

    Args:
        timezone_str: 예 ``\"Asia/Seoul\"``, ``\"America/Los_Angeles\"`` .

    Returns:
        동경 기준 UTC에 더할 분. 예: 한국 +540, LA 표준(PST) -480.

    Raises:
        ValueError: 구역 문자열이 잘못되었을 때.
    """
    text = (timezone_str or "").strip()
    if not text:
        raise ValueError("timezone_str이 비어 있습니다.")
    try:
        z = ZoneInfo(text)
        anchor = datetime(2000, 1, 15, 12, 0, 0, tzinfo=z)
        off = anchor.utcoffset()
        if off is None:
            raise ValueError(f"시간대 {timezone_str!r} 에서 utcoffset 을 얻을 수 없습니다.")
        return int(off.total_seconds() // 60)
    except ZoneInfoNotFoundError:
        if text in _FALLBACK_STANDARD_EAST_MINUTES:
            return _FALLBACK_STANDARD_EAST_MINUTES[text]
        raise ValueError(
            f"IANA 시간대 {timezone_str!r} 를 불러올 수 없습니다. "
            "`pip install tzdata` 로 OS 시간대 데이터를 설치하거나, "
            "지원 목록에 구역을 추가하세요."
        ) from None


def standard_meridian_longitude_degrees(timezone_str: str) -> float:
    """
    표준 자오선 경도(동경 양수)를 도 단위로 반환합니다.

    관례적으로 ``표준시 UTC 오프셋(시) × 15°`` 로 둡니다.
    (예: UTC+9 → 135°, UTC−8 → −120°.)
    """
    east_min = standard_utc_offset_east_minutes(timezone_str)
    return (east_min / 60.0) * 15.0


def calculate_apparent_solar_time(
    local_hour: int,
    local_minute: int,
    longitude: float,
    timezone_str: str,
) -> Tuple[int, int, int]:
    """
    사용자가 입력한 **시계 시각**에 진태양시(경도·표준자오선) 보정만 더합니다.

    보정 분식(원광과 동일한 선형 근사):
    ``(출생지경도 − 시간대표준자오선경도) × 4`` 분.

    DST는 보정에 끼어들지 않으며, ``timezone_str`` 의 표준 자오선만 사용합니다.

    Args:
        local_hour: 0~23 (시계 그대로).
        local_minute: 0~59.
        longitude: 출생지 경도(동경 +, 서경 −), −180~180.
        timezone_str: IANA 시간대.

    Returns:
        ``(day_delta, hour, minute)`` — 입력한 **양력 일자**를 0일로 두었을 때,
        보정으로 하루가 넘어가면 ``day_delta`` 가 ``-1`` 또는 ``+1`` 등이 됩니다.

    Raises:
        ValueError: 시·분·경도·시간대가 범위를 벗어날 때.
    """
    if not (0 <= local_hour <= 23):
        raise ValueError(f"local_hour는 0~23이어야 합니다. 입력: {local_hour}")
    if not (0 <= local_minute <= 59):
        raise ValueError(f"local_minute는 0~59이어야 합니다. 입력: {local_minute}")
    if not (-180.0 <= longitude <= 180.0):
        raise ValueError(f"longitude는 -180~180이어야 합니다. 입력: {longitude}")

    std_mer = standard_meridian_longitude_degrees(timezone_str)
    # 한글 주석 의도: 경도 1°당 약 4분 — 지방시와 표준시의 차이를 선형으로 환산.
    correction_min = int(round((longitude - std_mer) * 4.0))
    total_minutes = local_hour * 60 + local_minute + correction_min
    day_delta, rem = divmod(total_minutes, 1440)
    return (day_delta, rem // 60, rem % 60)


def _run_self_tests() -> None:
    """원광 검증용 고정 예시(서울·LA·NYC)를 출력합니다."""
    print("=== solar_time.py 자체 테스트 (원광 호환) ===")
    # 서울 14:00 → 13:28 (-32분)
    dd, h, m = calculate_apparent_solar_time(14, 0, 126.978, "Asia/Seoul")
    assert (dd, h, m) == (0, 13, 28), (dd, h, m)
    print(f"서울 14:00 → day_delta={dd}, {h:02d}:{m:02d} (기대 13:28)")
    # LA 14:30 → 14:37 (+7분)
    dd, h, m = calculate_apparent_solar_time(14, 30, -118.24, "America/Los_Angeles")
    assert (dd, h, m) == (0, 14, 37), (dd, h, m)
    print(f"LA 14:30 → day_delta={dd}, {h:02d}:{m:02d} (기대 14:37)")
    # NYC 09:00 → 09:04 (+4분)
    dd, h, m = calculate_apparent_solar_time(9, 0, -74.0, "America/New_York")
    assert (dd, h, m) == (0, 9, 4), (dd, h, m)
    print(f"NYC 09:00 → day_delta={dd}, {h:02d}:{m:02d} (기대 09:04)")
    print("=== solar_time.py 자체 테스트 통과 ===")


if __name__ == "__main__":
    _run_self_tests()
