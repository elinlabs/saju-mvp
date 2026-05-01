# -*- coding: utf-8 -*-
"""
사주 계산에 사용하는 천간·지지·절기·오호둔·오서둔 등 룩업 테이블과 보조 함수 모음.

만세력/절기 시각은 별도 데이터와 결합해 사용하며, 본 모듈은 표준 매핑과
시각(시·분) 구간에 따른 시지 판정만 담당합니다.
"""

from __future__ import annotations

import re
from typing import Dict, Final, List

# ---------------------------------------------------------------------------
# 1. 60갑자 기본 리스트
# ---------------------------------------------------------------------------

HEAVENLY_STEMS: Final[List[str]] = ["갑", "을", "병", "정", "무", "기", "경", "신", "임", "계"]
"""천간(天干) 10개를 전통 순서(갑·을·병·…)로 나열한 불변 리스트입니다."""

EARTHLY_BRANCHES: Final[List[str]] = ["자", "축", "인", "묘", "진", "사", "오", "미", "신", "유", "술", "해"]
"""지지(地支) 12개를 전통 순서(자·축·인·…)로 나열한 불변 리스트입니다."""

SIXTY_GANJI: Final[List[str]] = [
    HEAVENLY_STEMS[i % 10] + EARTHLY_BRANCHES[i % 12] for i in range(60)
]
"""
60갑자(六十甲子) 전체를 갑자부터 계해까지 순서대로 나열한 리스트입니다.

같은 인덱스 i에 대해 천간은 i % 10, 지지는 i % 12로 맞추면
전통적인 60주기(10과 12의 최소공배수 60)와 일치합니다.
"""

# ---------------------------------------------------------------------------
# 2. 五虎遁 (오호둔) — 연간(日干이 아닌 연간)에 따른 인월(寅月) 월간 시작 천간
# ---------------------------------------------------------------------------

FIVE_TIGER_RULE: Final[Dict[str, str]] = {
    "갑": "병",
    "기": "병",
    "을": "무",
    "경": "무",
    "병": "경",
    "신": "경",
    "정": "임",
    "임": "임",
    "무": "갑",
    "계": "갑",
}
"""
오호둔(五虎遁) 규칙: 연간(年干)에 따라 절기월의 인월(寅月)에서 시작하는 월간(月干) 천간을 돌립니다.

예를 들어 갑년·기년에는 인월이 병인월로 시작하는 식으로,
사주에서 월주 천간을 절기 구간과 함께 도출할 때 참조합니다.
"""

# ---------------------------------------------------------------------------
# 3. 五鼠遁 (오서둔) — 일간에 따른 자시(子時) 시각의 시간(時干) 시작 천간
# ---------------------------------------------------------------------------

FIVE_RAT_RULE: Final[Dict[str, str]] = {
    "갑": "갑",
    "기": "갑",
    "을": "병",
    "경": "병",
    "병": "무",
    "신": "무",
    "정": "경",
    "임": "경",
    "무": "임",
    "계": "임",
}
"""
오서둔(五鼠遁) 규칙: 일간(日干)에 따라 자시(子時) 구간에서 시작하는 시간(時干)의 천간을 돌립니다.

일간이 갑·기이면 자시는 갑자시로 시작하는 등, 시주 계산 시 자시 기준 시천간을 정할 때 사용합니다.
"""

# ---------------------------------------------------------------------------
# 4. 절기명 → 월지(月支) — 절입월의 지지 한 글자
# ---------------------------------------------------------------------------

SOLAR_TERM_TO_MONTH_BRANCH: Final[Dict[str, str]] = {
    "입춘": "인",
    "경칩": "묘",
    "청명": "진",
    "입하": "사",
    "망종": "오",
    "소서": "미",
    "입추": "신",
    "백로": "유",
    "한로": "술",
    "입동": "해",
    "대설": "자",
    "소한": "축",
}
"""
중기 절기(節氣) 이름을 음력이 아닌 절기월의 월지(月支) 한 글자에 대응시킵니다.

입춘부터 인월, 경칩 묘월 … 소한 축월까지 12절기(월의 시작을 나타내는 절)와
인·묘·진·…·축의 대응을 고정합니다. 절기 시각 데이터와 함께 월지를 판정할 때 사용합니다.
"""


def get_hour_branch(hour: int, minute: int) -> str:
    """
    시·분을 받아 해당 시각이 속하는 시지(時支) 한 글자를 반환합니다.

    - **자시**: 야자·조자 규칙을 유지합니다. 23:30~23:59 또는 00:00~01:29.
    - **축~묘**: 기존처럼 **정각+30분** 시작(예: 축 01:30~03:29).
    - **진~해**: 원광 만세력 시주 검증에 맞추어 **정각 기준 2시간** 블록으로 둡니다.
      (예: 사 09:00~10:59, 미 13:00~14:59, 해 21:00~23:29 — 23:30부터는 자시)

    Args:
        hour: 시(0~23).
        minute: 분(0~59).

    Returns:
        시지 문자열(예: "자", "축").

    Raises:
        ValueError: 시·분이 허용 범위를 벗어난 경우.
    """
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"시·분이 유효하지 않습니다: hour={hour}, minute={minute}")

    total_minutes = hour * 60 + minute

    # 자시: 23:30~23:59 또는 00:00~01:29
    if total_minutes >= 23 * 60 + 30 or total_minutes <= 1 * 60 + 29:
        return "자"
    # 축~묘: 정각+30분 시작(기존과 동일)
    if 1 * 60 + 30 <= total_minutes <= 3 * 60 + 29:
        return "축"
    if 3 * 60 + 30 <= total_minutes <= 5 * 60 + 29:
        return "인"
    if 5 * 60 + 30 <= total_minutes <= 7 * 60 + 29:
        return "묘"
    # 진~해: 정각 기준 2시간(원광 시지 표기와 동일 축)
    if 7 * 60 + 30 <= total_minutes <= 8 * 60 + 59:
        return "진"
    if 9 * 60 + 0 <= total_minutes <= 10 * 60 + 59:
        return "사"
    if 11 * 60 + 0 <= total_minutes <= 12 * 60 + 59:
        return "오"
    if 13 * 60 + 0 <= total_minutes <= 14 * 60 + 59:
        return "미"
    if 15 * 60 + 0 <= total_minutes <= 16 * 60 + 59:
        return "신"
    if 17 * 60 + 0 <= total_minutes <= 18 * 60 + 59:
        return "유"
    if 19 * 60 + 0 <= total_minutes <= 20 * 60 + 59:
        return "술"
    if 21 * 60 + 0 <= total_minutes <= 23 * 60 + 29:
        return "해"

    # 위 구간 밖(이론상 7:00~7:29 등)은 만세력에서 쓰이지 않는 공백이면 예외
    raise ValueError(f"시지를 판정할 수 없는 시각입니다: {hour:02d}:{minute:02d}")


# 한글 간지 + 선택적 한자 괄호 — 앞쪽 한글 간지에서만 추출
_GANJI_KOREAN_PREFIX_PATTERN = re.compile(
    r"^([갑을병정무기경신임계])([자축인묘진사오미신유술해])"
)


def extract_stem(ganji: str) -> str:
    """
    음양력 CSV 등에 쓰이는 '갑자(甲子)' 형태 문자열에서 천간(天干) 한 글자만 추출합니다.

    괄호 안 한자는 무시하고, 문자열 앞의 한글 간지 두 글자 중 첫 글자를 천간으로 반환합니다.

    Args:
        ganji: 예) '갑자(甲子)', '을축'.

    Returns:
        천간 한 글자.

    Raises:
        ValueError: 앞부분에서 유효한 한글 간지를 찾지 못한 경우.
    """
    normalized = (ganji or "").strip()
    if not normalized:
        raise ValueError("빈 문자열에서는 천간을 추출할 수 없습니다.")
    match = _GANJI_KOREAN_PREFIX_PATTERN.search(normalized)
    if not match:
        raise ValueError(f"한글 간지 형식을 인식할 수 없습니다: {ganji!r}")
    stem = match.group(1)
    if stem not in HEAVENLY_STEMS:
        raise ValueError(f"인식된 천간이 목록에 없습니다: {stem!r}")
    return stem


def extract_branch(ganji: str) -> str:
    """
    음양력 CSV 등에 쓰이는 '갑자(甲子)' 형태 문자열에서 지지(地支) 한 글자만 추출합니다.

    괄호 안 한자는 무시하고, 문자열 앞의 한글 간지 두 글자 중 둘째 글자를 지지로 반환합니다.

    Args:
        ganji: 예) '갑자(甲子)', '을축'.

    Returns:
        지지 한 글자.

    Raises:
        ValueError: 앞부분에서 유효한 한글 간지를 찾지 못한 경우.
    """
    normalized = (ganji or "").strip()
    if not normalized:
        raise ValueError("빈 문자열에서는 지지를 추출할 수 없습니다.")
    match = _GANJI_KOREAN_PREFIX_PATTERN.search(normalized)
    if not match:
        raise ValueError(f"한글 간지 형식을 인식할 수 없습니다: {ganji!r}")
    branch = match.group(2)
    if branch not in EARTHLY_BRANCHES:
        raise ValueError(f"인식된 지지가 목록에 없습니다: {branch!r}")
    return branch


def _run_self_tests() -> None:
    """모듈 단독 실행 시 테이블·함수의 기대값을 검증하고 요약을 출력합니다."""
    print("=== tables.py 자체 테스트 ===")

    assert len(HEAVENLY_STEMS) == 10
    assert len(EARTHLY_BRANCHES) == 12
    assert len(SIXTY_GANJI) == 60
    assert SIXTY_GANJI[0] == "갑자", SIXTY_GANJI[-1] == "계해"
    print(f"60갑자: 첫={SIXTY_GANJI[0]}, 끝={SIXTY_GANJI[-1]}, 개수={len(SIXTY_GANJI)}")

    assert set(FIVE_TIGER_RULE.keys()) == set(HEAVENLY_STEMS)
    assert all(v in HEAVENLY_STEMS for v in FIVE_TIGER_RULE.values())
    print("오호둔: 10천간 키 전부 존재, 값 모두 천간 목록 내")

    assert set(FIVE_RAT_RULE.keys()) == set(HEAVENLY_STEMS)
    assert all(v in HEAVENLY_STEMS for v in FIVE_RAT_RULE.values())
    print("오서둔: 10천간 키 전부 존재, 값 모두 천간 목록 내")

    assert len(SOLAR_TERM_TO_MONTH_BRANCH) == 12
    expected_branch_cycle = ["인", "묘", "진", "사", "오", "미", "신", "유", "술", "해", "자", "축"]
    terms_order = [
        "입춘",
        "경칩",
        "청명",
        "입하",
        "망종",
        "소서",
        "입추",
        "백로",
        "한로",
        "입동",
        "대설",
        "소한",
    ]
    for term, expected_branch in zip(terms_order, expected_branch_cycle):
        assert SOLAR_TERM_TO_MONTH_BRANCH[term] == expected_branch
    print("절기→월지: 12절기 순서와 인~축 대응 일치")

    # 시지 경계 샘플(자·축~묘·진~해 규칙 반영)
    assert get_hour_branch(23, 30) == "자"
    assert get_hour_branch(0, 0) == "자"
    assert get_hour_branch(1, 29) == "자"
    assert get_hour_branch(1, 30) == "축"
    assert get_hour_branch(3, 29) == "축"
    assert get_hour_branch(3, 30) == "인"
    assert get_hour_branch(7, 29) == "묘"
    assert get_hour_branch(7, 30) == "진"
    assert get_hour_branch(8, 59) == "진"
    assert get_hour_branch(9, 0) == "사"
    assert get_hour_branch(11, 0) == "오"
    assert get_hour_branch(18, 59) == "유"
    assert get_hour_branch(19, 0) == "술"
    assert get_hour_branch(20, 59) == "술"
    assert get_hour_branch(21, 0) == "해"
    assert get_hour_branch(23, 29) == "해"
    assert get_hour_branch(23, 30) == "자"
    print("시지 구간: 경계값(자/축/…/해/자) 검증 통과")

    assert extract_stem("갑자(甲子)") == "갑"
    assert extract_branch("갑자(甲子)") == "자"
    assert extract_stem("을축") == "을"
    assert extract_branch("을축") == "축"
    print("천간·지지 추출: 갑자(甲子), 을축 예시 통과")

    print("=== 모든 자체 테스트 통과 ===")


if __name__ == "__main__":
    _run_self_tests()
