"""
BTS Airline On-Time Performance Data Downloader (PREZIP Direct Link Version)
=======================================================================
BTS provides direct download links for all historical data at https://transtats.bts.gov/PREZIP/,
with fixed filename format, no browser session required, can download directly with requests.

File naming pattern:
    On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{YEAR}_{MONTH}.zip
    Example: On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip

Install dependencies:
    pip install requests tqdm

Usage:
    # Test with small range first (recommended to confirm download works)
    python download_bts_data.py --start 2024-01 --end 2024-03 --output ./raw_data

    # Download full historical data (takes several hours)
    python download_bts_data.py --start 2000-01 --end 2025-12 --output ./raw_data

    # Increase speed (reduce wait time, but don't be too aggressive)
    python download_bts_data.py --start 2020-01 --end 2024-12 --delay 1.0
"""

import time
import argparse
import logging
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("download_bts.log"),
    ],
)
logger = logging.getLogger(__name__)

# ── PREZIP Direct Link Configuration ──────────────────────────────────────────────────────────
BASE_URL = "https://transtats.bts.gov/PREZIP"
FILE_TEMPLATE = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.transtats.bts.gov/",
}


def generate_months(start: str, end: str):
    """Generate all (year, month) tuples in the range from start to end"""
    start_dt = datetime.strptime(start, "%Y-%m")
    end_dt   = datetime.strptime(end,   "%Y-%m")
    year, month = start_dt.year, start_dt.month
    while (year, month) <= (end_dt.year, end_dt.month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def download_month(
    year: int,
    month: int,
    output_dir: Path,
    session: requests.Session,
    retry: int = 3,
    delay: float = 2.0,
) -> bool:
    """Download single month data, return success status"""
    filename = FILE_TEMPLATE.format(year=year, month=month)
    url      = f"{BASE_URL}/{filename}"
    out_path = output_dir / f"ontime_{year}_{month:02d}.zip"

    # Resume from breakpoint: skip if already exists
    if out_path.exists() and out_path.stat().st_size > 0:
        logger.info(f"  ⏭  Skipped (already exists): {out_path.name}")
        return True

    for attempt in range(1, retry + 1):
        try:
            logger.info(f"  ⬇  {year}-{month:02d}  {url}  (Attempt {attempt})")
            resp = session.get(url, headers=HEADERS, timeout=120, stream=True)

            if resp.status_code == 404:
                logger.warning(f"  ⚠  404 Not Found (month may not be published yet): {filename}")
                return False

            resp.raise_for_status()

            # Stream write, show progress bar
            total = int(resp.headers.get("Content-Length", 0))
            with open(out_path, "wb") as f:
                with tqdm(
                    total=total, unit="B", unit_scale=True,
                    desc=f"    {year}-{month:02d}", leave=False,
                ) as pbar:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                        pbar.update(len(chunk))

            size_mb = out_path.stat().st_size / 1024 / 1024
            logger.info(f"  ✅  {out_path.name} ({size_mb:.1f} MB)")
            time.sleep(delay)
            return True

        except requests.exceptions.RequestException as e:
            logger.warning(f"  ❌  Attempt {attempt} failed: {e}")
            # Delete potentially incomplete file
            if out_path.exists():
                out_path.unlink()
            if attempt < retry:
                wait = delay * (2 ** attempt)
                logger.info(f"     Waiting {wait:.0f}s before retry...")
                time.sleep(wait)

    logger.error(f"  💀  {year}-{month:02d} all retries failed")
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Batch download airline on-time performance data from BTS PREZIP server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--start",  default="2024-01", help="Start year-month YYYY-MM (default 2024-01)")
    parser.add_argument("--end",    default="2024-12", help="End year-month YYYY-MM (default 2024-12)")
    parser.add_argument("--output", default="./raw_data", help="Output directory (default ./raw_data)")
    parser.add_argument("--delay",  type=float, default=2.0,
                        help="Seconds to wait after each download to avoid rate limiting (default 2.0)")
    parser.add_argument("--retry",  type=int,  default=3,
                        help="Number of retry attempts on failure (default 3)")
    parser.add_argument("--workers", type=int,  default=1,
                        help="Number of concurrent download threads, recommend no more than 4 (default 1)")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    months = list(generate_months(args.start, args.end))
    logger.info(f"Total {len(months)} months ({args.start} ~ {args.end})")

    existing = sum(
        1 for y, m in months
        if (output_dir / f"ontime_{y}_{m:02d}.zip").exists()
    )
    if existing:
        logger.info(f"{existing} files already exist, will be skipped")

    # Estimate data size
    est_zip_gb  = len(months) * 45 / 1024
    est_csv_gb  = est_zip_gb * 10
    logger.info(f"Estimated ZIP total size: ~{est_zip_gb:.1f} GB, after extraction: ~{est_csv_gb:.0f} GB")

    if args.workers > 1:
        # Multi-threaded download
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger.info(f"Using {args.workers} concurrent threads")
        success, failed = 0, []

        def _dl(ym):
            y, m = ym
            s = requests.Session()
            ok = download_month(y, m, output_dir, s, retry=args.retry, delay=args.delay)
            s.close()
            return (y, m, ok)

        with ThreadPoolExecutor(max_workers=args.workers) as exe:
            futures = {exe.submit(_dl, ym): ym for ym in months}
            for fut in tqdm(as_completed(futures), total=len(months), desc="Overall progress"):
                y, m, ok = fut.result()
                if ok:
                    success += 1
                else:
                    failed.append(f"{y}-{m:02d}")
    else:
        # Single-threaded download
        success, failed = 0, []
        with requests.Session() as session:
            for i, (year, month) in enumerate(months, 1):
                logger.info(f"\n[{i}/{len(months)}]")
                ok = download_month(
                    year, month, output_dir, session,
                    retry=args.retry, delay=args.delay,
                )
                if ok:
                    success += 1
                else:
                    failed.append(f"{year}-{month:02d}")

    # Summary
    logger.info("\n" + "=" * 55)
    logger.info(f"Completed: {success}/{len(months)} successful")
    if failed:
        logger.warning(f"Failed ({len(failed)}): {', '.join(failed)}")
        fail_file = output_dir / "failed_months.txt"
        fail_file.write_text("\n".join(failed))
        logger.info(f"Failed list saved: {fail_file}")
        logger.info("Re-run the script to retry (successful files will be skipped)")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()