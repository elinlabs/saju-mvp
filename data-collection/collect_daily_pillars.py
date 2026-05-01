# 사용법: .env에 KASI_API_KEY를 설정하고 START_YEAR/END_YEAR를 확인한 뒤 실행하세요.
import argparse
import csv
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import xml.etree.ElementTree as ET


def ensure_package(package_name: str, import_name: Optional[str] = None) -> None:
    """필수 라이브러리가 없으면 pip로 자동 설치합니다."""
    module_name = import_name or package_name
    try:
        __import__(module_name)
    except ImportError:
        print(f"필수 라이브러리 '{package_name}'가 없어 자동 설치를 시도합니다...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])


ensure_package("requests")
ensure_package("python-dotenv", "dotenv")

import requests
from dotenv import load_dotenv


API_URL = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService/getLunCalInfo"
START_YEAR = 2014
END_YEAR = 2027
SLEEP_SECONDS = 1.0
RETRY_BACKOFF_SECONDS = [2, 4, 8, 16, 32]

OUTPUT_DIR = Path("output")
OUTPUT_CSV = OUTPUT_DIR / f"daily_pillars_{START_YEAR}_{END_YEAR}.csv"
ERROR_LOG = OUTPUT_DIR / "errors.log"
DATE_PATTERN = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
LOG_TARGET_DATE_PATTERN = re.compile(
    r"^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]\s+(\d{4}-\d{2}-\d{2})\b"
)

LIMIT_ERROR_CODE = "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR"
CSV_FIELDS = [
    "solar_date",
    "lun_year",
    "lun_month",
    "lun_day",
    "lun_leapmonth",
    "year_pillar",
    "day_pillar",
    "day_of_week",
    "julian_day",
]


def safe_text(parent: ET.Element, tag: str) -> str:
    """XML 노드에서 태그 텍스트를 안전하게 읽습니다."""
    node = parent.find(tag)
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def write_error(message: str) -> None:
    """오류 메시지를 타임스탬프와 함께 errors.log에 기록합니다."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with ERROR_LOG.open("a", encoding="utf-8") as fp:
        fp.write(f"[{timestamp}] {message}\n")


def parse_retry_after_seconds(response: Optional[requests.Response]) -> Optional[int]:
    """Retry-After 헤더를 초 단위 정수로 파싱해 반환합니다."""
    if response is None:
        return None
    raw_value = response.headers.get("Retry-After", "").strip()
    if not raw_value:
        return None
    try:
        parsed = int(raw_value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def fetch_day_data_once(
    session: requests.Session, service_key: str, target_date: date
) -> Tuple[str, Dict[str, str], str, Optional[int]]:
    """특정 날짜 데이터를 1회 호출해 상태/행/사유/Retry-After를 반환합니다."""
    params = {
        "solYear": f"{target_date.year:04d}",
        "solMonth": f"{target_date.month:02d}",
        "solDay": f"{target_date.day:02d}",
        "ServiceKey": service_key,
    }

    try:
        response = session.get(API_URL, params=params, timeout=20)
        response.raise_for_status()
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        retry_after = parse_retry_after_seconds(exc.response)
        if status_code == 429:
            return "retryable_429", {}, f"HTTPError 429: {exc}", retry_after
        return "error", {}, f"HTTPError: {exc}", None
    except requests.RequestException as exc:
        return "error", {}, f"요청 실패: {exc}", None

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError as exc:
        return "error", {}, f"XML 파싱 실패: {exc}", None

    result_code = root.findtext(".//header/resultCode", default="").strip()
    result_msg = root.findtext(".//header/resultMsg", default="").strip()
    if result_code:
        if result_code == LIMIT_ERROR_CODE:
            return "limit", {}, f"일일 한도 초과: code={result_code}, msg={result_msg}", None
        if result_code != "00":
            return "error", {}, f"API 오류: code={result_code}, msg={result_msg}", None

    total_count_text = root.findtext(".//body/totalCount", default="0").strip()
    try:
        total_count = int(total_count_text)
    except ValueError:
        total_count = 0

    if total_count == 0:
        return "empty", {}, "빈 응답(totalCount=0)", None

    item = root.find(".//body/items/item")
    if item is None:
        return "empty", {}, "응답에 item 없음", None

    row = {
        "solar_date": target_date.strftime("%Y-%m-%d"),
        "lun_year": safe_text(item, "lunYear"),
        "lun_month": safe_text(item, "lunMonth"),
        "lun_day": safe_text(item, "lunDay"),
        "lun_leapmonth": "윤" if safe_text(item, "lunLeapmonth") == "윤" else "평",
        "year_pillar": safe_text(item, "lunSecha"),
        "day_pillar": safe_text(item, "lunIljin"),
        "day_of_week": safe_text(item, "solWeek"),
        "julian_day": safe_text(item, "solJd"),
    }
    return "ok", row, "", None


def fetch_day_data_with_retry(
    session: requests.Session, service_key: str, target_date: date
) -> Tuple[str, Dict[str, str], str, int]:
    """429 발생 시 최대 5회 지수 백오프로 재시도한 최종 결과를 반환합니다."""
    retry_count = 0
    max_retries = len(RETRY_BACKOFF_SECONDS)

    while True:
        status, row, reason, retry_after = fetch_day_data_once(session, service_key, target_date)
        if status != "retryable_429":
            return status, row, reason, retry_count

        if retry_count >= max_retries:
            return (
                "error",
                {},
                f"{reason} / 429 재시도 {max_retries}회 모두 실패",
                retry_count,
            )

        wait_seconds = retry_after if retry_after is not None else RETRY_BACKOFF_SECONDS[retry_count]
        retry_count += 1
        print(
            f"{target_date.strftime('%Y-%m-%d')} 429 재시도 {retry_count}/{max_retries} "
            f"(대기 {wait_seconds}초)"
        )
        time.sleep(wait_seconds)


def daterange(start: date, end: date) -> Iterable[date]:
    """시작일부터 종료일까지 하루 단위 날짜를 순회합니다."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def load_existing_rows(csv_path: Path) -> Dict[str, Dict[str, str]]:
    """기존 CSV를 읽어 solar_date를 키로 한 딕셔너리로 반환합니다."""
    if not csv_path.exists():
        return {}
    rows_by_date: Dict[str, Dict[str, str]] = {}
    with csv_path.open("r", newline="", encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            solar_date = (row.get("solar_date") or "").strip()
            if not solar_date:
                continue
            rows_by_date[solar_date] = {field: row.get(field, "") for field in CSV_FIELDS}
    return rows_by_date


def save_rows(csv_path: Path, rows_by_date: Dict[str, Dict[str, str]]) -> None:
    """행 데이터를 날짜순으로 정렬해 CSV로 저장합니다."""
    sorted_rows = [rows_by_date[key] for key in sorted(rows_by_date.keys())]
    with csv_path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(sorted_rows)


def load_failed_dates_from_log(start_date: date, end_date: date) -> List[date]:
    """errors.log에서 실패 날짜를 추출해 중복 제거 후 날짜순으로 반환합니다."""
    if not ERROR_LOG.exists():
        return []
    unique_dates = set()
    with ERROR_LOG.open("r", encoding="utf-8") as fp:
        for line in fp:
            # 로그 라인의 첫 날짜는 기록 시각이므로, 실제 실패 대상 날짜(두 번째 날짜)를 우선 추출합니다.
            target_match = LOG_TARGET_DATE_PATTERN.search(line)
            if target_match:
                candidate_dates = [target_match.group(1)]
            else:
                # 구버전/수동 로그 등 형식이 다를 수 있어 모든 날짜 토큰을 후보로 처리합니다.
                candidate_dates = DATE_PATTERN.findall(line)
            parsed: Optional[date] = None
            for candidate in candidate_dates:
                try:
                    parsed = datetime.strptime(candidate, "%Y-%m-%d").date()
                    break
                except ValueError:
                    continue
            if parsed is None:
                continue
            if start_date <= parsed <= end_date:
                unique_dates.add(parsed)
    return sorted(unique_dates)


def load_dates_from_file(file_path: Path, start_date: date, end_date: date) -> List[date]:
    """지정 파일에서 YYYY-MM-DD 날짜를 읽어 유효 범위만 중복 제거 후 반환합니다."""
    if not file_path.exists():
        print(f"오류: 지정한 파일이 없습니다: {file_path}")
        sys.exit(1)

    unique_dates = set()
    with file_path.open("r", encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            raw = line.strip()
            if not raw:
                continue
            if not DATE_PATTERN.fullmatch(raw):
                print(f"오류: 날짜 형식이 올바르지 않습니다 ({file_path}:{line_no}) -> {raw}")
                sys.exit(1)
            try:
                parsed = datetime.strptime(raw, "%Y-%m-%d").date()
            except ValueError:
                print(f"오류: 유효하지 않은 날짜입니다 ({file_path}:{line_no}) -> {raw}")
                sys.exit(1)
            if start_date <= parsed <= end_date:
                unique_dates.add(parsed)

    return sorted(unique_dates)


def print_start_eta(total_calls: int) -> None:
    """시작 시각과 대략적인 예상 완료 시각을 출력합니다."""
    started_at = datetime.now()
    estimated_seconds = total_calls * SLEEP_SECONDS
    estimated_done = started_at + timedelta(seconds=estimated_seconds)
    estimated_minutes = round(estimated_seconds / 60)
    print(
        "시작: "
        f"{started_at.strftime('%Y-%m-%d %H:%M')} / "
        f"예상 완료: 약 {estimated_done.strftime('%H:%M')} "
        f"({estimated_minutes}분 소요 예상)"
    )


def parse_args() -> argparse.Namespace:
    """명령행 인자를 파싱합니다."""
    parser = argparse.ArgumentParser(
        description="한국천문연 음양력 API에서 일별 사주 데이터를 수집합니다."
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="errors.log의 실패 날짜만 재수집해 기존 CSV에 병합합니다.",
    )
    parser.add_argument(
        "--retry-from-file",
        type=Path,
        help="지정 파일(YYYY-MM-DD 한 줄씩)의 날짜만 재수집해 기존 CSV에 병합합니다.",
    )
    return parser.parse_args()


def main() -> None:
    """전체 수집/재수집 흐름을 실행하고 결과 통계를 출력합니다."""
    args = parse_args()
    if args.retry_failed and args.retry_from_file:
        print("오류: --retry-failed 와 --retry-from-file 은 동시에 사용할 수 없습니다.")
        sys.exit(1)

    load_dotenv()
    api_key = os.getenv("KASI_API_KEY", "").strip()
    if not api_key:
        print("오류: .env 파일에 KASI_API_KEY를 설정해 주세요.")
        sys.exit(1)

    start_date = date(START_YEAR, 1, 1)
    end_date = date(END_YEAR, 12, 31)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not args.retry_failed and not args.retry_from_file and ERROR_LOG.exists():
        ERROR_LOG.unlink()

    if args.retry_from_file:
        target_dates = load_dates_from_file(args.retry_from_file, start_date, end_date)
        rows_by_date = load_existing_rows(OUTPUT_CSV)
        mode_text = f"파일 지정 재시도 모드 ({args.retry_from_file})"
    elif args.retry_failed:
        target_dates = load_failed_dates_from_log(start_date, end_date)
        rows_by_date = load_existing_rows(OUTPUT_CSV)
        mode_text = "실패 날짜 재시도 모드"
    else:
        target_dates = list(daterange(start_date, end_date))
        rows_by_date = {}
        mode_text = "전체 수집 모드"

    total_calls = len(target_dates)
    print(f"모드: {mode_text}")
    print_start_eta(total_calls)

    if total_calls == 0:
        print("처리할 날짜가 없어 종료합니다.")
        return

    success_calls = 0
    failed_calls = 0
    empty_calls = 0
    last_success_date: Optional[date] = None
    limit_stopped = False

    with requests.Session() as session:
        for idx, target_date in enumerate(target_dates, start=1):
            status, row, reason, retry_count = fetch_day_data_with_retry(session, api_key, target_date)

            if status == "ok":
                rows_by_date[target_date.strftime("%Y-%m-%d")] = row
                success_calls += 1
                last_success_date = target_date
            elif status == "empty":
                empty_calls += 1
                failed_calls += 1
                write_error(f"{target_date.strftime('%Y-%m-%d')} {reason}")
            elif status == "limit":
                failed_calls += 1
                write_error(f"{target_date.strftime('%Y-%m-%d')} {reason}")
                print("\n일일 호출 한도 초과로 수집을 즉시 중단합니다.")
                limit_stopped = True
                print(
                    f"{target_date.strftime('%Y-%m-%d')} [{idx}/{total_calls}] "
                    f"성공:{success_calls} 실패:{failed_calls} 재시도중:{retry_count}"
                )
                break
            else:
                failed_calls += 1
                write_error(f"{target_date.strftime('%Y-%m-%d')} {reason}")

            print(
                f"{target_date.strftime('%Y-%m-%d')} [{idx}/{total_calls}] "
                f"성공:{success_calls} 실패:{failed_calls} 재시도중:{retry_count}"
            )
            time.sleep(SLEEP_SECONDS)

    save_rows(OUTPUT_CSV, rows_by_date)

    print("\n작업 완료")
    print(f"- 수집 기간: {start_date} ~ {end_date}")
    print(f"- 실행 모드: {mode_text}")
    print(f"- 총 대상 호출 수: {total_calls}")
    print(f"- 성공 호출: {success_calls}")
    print(f"- 실패 호출: {failed_calls}")
    print(f"- 빈 응답 수: {empty_calls}")
    print(f"- 저장된 행 수: {len(rows_by_date)}")
    print(f"- CSV 파일: {OUTPUT_CSV.resolve()}")
    if failed_calls > 0:
        print(f"- 오류 로그: {ERROR_LOG.resolve()}")
    if limit_stopped:
        if last_success_date is not None:
            print(f"- 한도 초과 전 마지막 성공 일자: {last_success_date.strftime('%Y-%m-%d')}")
        else:
            print("- 한도 초과 전 성공 데이터가 없습니다.")


if __name__ == "__main__":
    main()
