# 사용법: .env에 KASI_API_KEY를 설정한 뒤 `python collect_solar_terms.py`로 실행하세요.
import csv
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
import xml.etree.ElementTree as ET


def ensure_package(package_name: str, import_name: Optional[str] = None) -> None:
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


API_URL = "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/get24DivisionsInfo"
START_YEAR = 2000
END_YEAR = 2027
TOTAL_CALLS = (END_YEAR - START_YEAR + 1) * 12
SLEEP_SECONDS = 0.3

OUTPUT_DIR = Path("output")
OUTPUT_CSV = OUTPUT_DIR / "solar_terms_2000_2027.csv"
ERROR_LOG = OUTPUT_DIR / "errors.log"


def safe_text(parent: ET.Element, tag: str) -> str:
    node = parent.find(tag)
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def parse_kst(kst: str) -> str:
    digits = "".join(ch for ch in (kst or "") if ch.isdigit())
    if len(digits) < 4:
        return ""
    hh = digits[:2]
    mm = digits[2:4]
    return f"{hh}:{mm}"


def build_datetime(locdate: str, kst: str) -> str:
    if len(locdate) != 8 or not locdate.isdigit():
        return ""
    time_part = parse_kst(kst)
    if not time_part:
        return ""
    year = locdate[0:4]
    month = locdate[4:6]
    day = locdate[6:8]
    return f"{year}-{month}-{day} {time_part}"


def write_error(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with ERROR_LOG.open("a", encoding="utf-8") as fp:
        fp.write(f"[{timestamp}] {message}\n")


def fetch_month_data(
    session: requests.Session, service_key: str, year: int, month: int
) -> Tuple[bool, List[dict], str]:
    month_str = f"{month:02d}"
    params = {
        "solYear": str(year),
        "solMonth": month_str,
        "ServiceKey": service_key,
    }

    try:
        response = session.get(API_URL, params=params, timeout=20)
        response.raise_for_status()
    except requests.RequestException as exc:
        return False, [], f"호출 실패: {exc}"

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError as exc:
        return False, [], f"XML 파싱 실패: {exc}"

    result_code = root.findtext(".//header/resultCode", default="").strip()
    result_msg = root.findtext(".//header/resultMsg", default="").strip()
    if result_code and result_code != "00":
        return False, [], f"API 오류(code={result_code}, msg={result_msg})"

    total_count_text = root.findtext(".//body/totalCount", default="0").strip()
    try:
        total_count = int(total_count_text)
    except ValueError:
        total_count = 0

    if total_count == 0:
        return False, [], "빈 응답(totalCount=0)"

    items = root.findall(".//body/items/item")
    records = []
    for item in items:
        locdate = safe_text(item, "locdate")
        kst = safe_text(item, "kst")
        year_text = locdate[0:4] if len(locdate) >= 4 else str(year)
        month_text = locdate[4:6] if len(locdate) >= 6 else month_str
        day_text = locdate[6:8] if len(locdate) >= 8 else ""
        date_value = f"{year_text}-{month_text}-{day_text}" if day_text else ""
        records.append(
            {
                "year": year_text,
                "month": month_text,
                "date": date_value,
                "datetime": build_datetime(locdate, kst),
                "dateName": safe_text(item, "dateName"),
                "sunLongitude": safe_text(item, "sunLongitude"),
            }
        )

    if not records:
        return False, [], "응답에 item 데이터 없음"

    return True, records, ""


def main() -> None:
    load_dotenv()
    api_key = os.getenv("KASI_API_KEY", "").strip()
    if not api_key:
        print("오류: .env 파일에 KASI_API_KEY를 설정해 주세요.")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if ERROR_LOG.exists():
        ERROR_LOG.unlink()

    all_rows: List[dict] = []
    success_calls = 0
    failed_calls = 0
    processed = 0

    with requests.Session() as session:
        for year in range(START_YEAR, END_YEAR + 1):
            for month in range(1, 13):
                processed += 1
                print(f"{year}년 {month}월 처리 중... [{processed}/{TOTAL_CALLS}]")
                ok, rows, reason = fetch_month_data(session, api_key, year, month)
                if ok:
                    success_calls += 1
                    all_rows.extend(rows)
                else:
                    failed_calls += 1
                    write_error(f"{year}-{month:02d} {reason}")
                time.sleep(SLEEP_SECONDS)

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=["year", "month", "date", "datetime", "dateName", "sunLongitude"],
        )
        writer.writeheader()
        writer.writerows(all_rows)

    print("\n작업 완료")
    print(f"- 전체 호출: {TOTAL_CALLS}")
    print(f"- 성공 호출: {success_calls}")
    print(f"- 실패 호출: {failed_calls}")
    print(f"- 저장된 행 수: {len(all_rows)}")
    print(f"- CSV 파일: {OUTPUT_CSV.resolve()}")
    if failed_calls > 0:
        print(f"- 오류 로그: {ERROR_LOG.resolve()}")


if __name__ == "__main__":
    main()
