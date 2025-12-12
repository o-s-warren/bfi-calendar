#!/usr/bin/env python3
"""
BFI Southbank Screening Scraper

Scrapes screening data from the BFI's AudienceView ticketing system.

Usage:
    python bfi_calendar.py fetch --days 14 --verbose
    python bfi_calendar.py serve --port 8000
    python bfi_calendar.py list --days 7 --venue NFT1 --available-only
    python bfi_calendar.py static --days 14 --output bfi.html
    python bfi_calendar.py cookies --diagnose

Author: Olivia
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import requests
from flask import Flask, render_template_string


# ============================================================
# LOGGING SETUP
# ============================================================

def setup_logging(verbose: bool = False, log_file: Optional[Path] = None) -> logging.Logger:
    """Configure logging to output to both terminal and file."""
    logger = logging.getLogger("bfi_scraper")
    logger.handlers.clear()
    
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    
    formatter = logging.Formatter(
        "%(asctime)s │ %(levelname)-8s │ %(message)s" if verbose else "%(message)s",
        datefmt="%H:%M:%S"
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file is None:
        log_file = Path("bfi_scraper.log")
    
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s │ %(levelname)-8s │ %(funcName)-25s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(file_handler)
    
    return logger


log = logging.getLogger("bfi_scraper")


# ============================================================
# CONFIGURATION
# ============================================================

BASE_URL = "https://whatson.bfi.org.uk/Online/default.asp"
ARTICLE_SEARCH_ID = "25E7EA2E-291F-44F9-8EBC-E560154FDAEB"
DATA_PATH = Path("bfi_screenings.json")
LOG_PATH = Path("bfi_scraper.log")
DEFAULT_DAYS_AHEAD = 14


# ============================================================
# DATA MODELS
# ============================================================

def normalise_venue_short(short: str) -> str:
    """Normalise BFI venue short names."""
    if not short:
        return ""
    s = short.strip()
    if s in ("Southbank NFT2 GA", "NFT2 GA"):
        return "NFT2"
    return s


UNWANTED_KEYWORDS = {
    "Closed captions", "Releases", "Audio description", "English subtitles",
    "Descriptive subtitles (open captions)", "Previews", "Relaxed (sensory) screenings",
}


def filter_keywords(keywords):
    """Remove platform-level metadata keywords."""
    return [k for k in keywords if k.strip() not in UNWANTED_KEYWORDS]


class Availability(Enum):
    """Ticket availability status codes."""
    EXCELLENT = "E"
    GOOD = "G"
    LIMITED = "L"
    SOLD_OUT = "S"
    UNKNOWN = "?"

    @classmethod
    def from_code(cls, code: str) -> "Availability":
        for member in cls:
            if member.value == code:
                return member
        return cls.UNKNOWN

    @property
    def display(self) -> str:
        return {
            self.EXCELLENT: "", self.GOOD: "",
            self.LIMITED: "Limited Tickets", self.SOLD_OUT: "Sold Out",
            self.UNKNOWN: "Unknown",
        }[self]

    @property
    def emoji(self) -> str:
        return {
            self.EXCELLENT: "🟢", self.GOOD: "🟡",
            self.LIMITED: "🟠", self.SOLD_OUT: "🔴", self.UNKNOWN: "⚪",
        }[self]


class SalesStatus(Enum):
    """Sales status codes."""
    ON_SALE = "S"
    NOT_ON_SALE = "N"
    UNKNOWN = "?"

    @classmethod
    def from_code(cls, code: str) -> "SalesStatus":
        for member in cls:
            if member.value == code:
                return member
        return cls.UNKNOWN


@dataclass
class Venue:
    """A screening venue within BFI Southbank."""
    id: str
    name: str
    short_name: str

    def __hash__(self):
        return hash(self.id)


@dataclass
class Screening:
    """A single film screening at the BFI."""
    id: str
    title: str
    datetime: dt.datetime
    venue: Venue
    availability: Availability
    sales_status: SalesStatus
    seats_available: Optional[int] = None
    keywords: list[str] = field(default_factory=list)
    min_price: Optional[str] = None
    max_price: Optional[str] = None
    article_url: Optional[str] = None

    @property
    def time_str(self) -> str:
        return self.datetime.strftime("%H:%M")

    @property
    def date_str(self) -> str:
        return self.datetime.strftime("%A %d %B %Y")

    @property
    def is_available(self) -> bool:
        return self.availability != Availability.SOLD_OUT

    @property
    def booking_url(self) -> str:
        if self.article_url:
            return f"https://whatson.bfi.org.uk/Online/{self.article_url}"
        return BASE_URL

    def to_dict(self) -> dict:
        """Serialise for JSON storage."""
        return {
            "id": self.id, "title": self.title,
            "datetime": self.datetime.isoformat(),
            "venue_id": self.venue.id, "venue_name": self.venue.name,
            "venue_short": self.venue.short_name,
            "availability": self.availability.value,
            "sales_status": self.sales_status.value,
            "seats_available": self.seats_available,
            "keywords": self.keywords,
            "min_price": self.min_price, "max_price": self.max_price,
            "article_url": self.article_url,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Screening":
        """Deserialise from JSON storage."""
        return cls(
            id=data["id"], title=data["title"],
            datetime=dt.datetime.fromisoformat(data["datetime"]),
            venue=Venue(
                id=data["venue_id"], name=data["venue_name"],
                short_name=normalise_venue_short(data["venue_short"]),
            ),
            availability=Availability.from_code(data["availability"]),
            sales_status=SalesStatus.from_code(data["sales_status"]),
            seats_available=data.get("seats_available"),
            keywords=filter_keywords(data.get("keywords", [])),
            min_price=data.get("min_price"), max_price=data.get("max_price"),
            article_url=data.get("article_url"),
        )


# ============================================================
# FIELD MAPPING
# ============================================================

class Fields:
    """Column indices for the searchResults array."""
    ID, TITLE, TIME, DAY, MONTH, YEAR = 0, 5, 8, 9, 10, 11
    SALES_STATUS, AVAILABILITY, SEATS_AVAILABLE = 14, 15, 16
    KEYWORDS, ARTICLE_URL = 17, 18
    VENUE_ID, VENUE_NAME, VENUE_SHORT = 62, 63, 64
    MIN_PRICE, MAX_PRICE = 80, 81


def parse_screening(row: list) -> Screening:
    """Parse a single row from searchResults into a Screening object."""
    screening_dt = dt.datetime(
        int(row[Fields.YEAR]),
        int(row[Fields.MONTH]) + 1,  # 0-indexed in source
        int(row[Fields.DAY]),
        int(row[Fields.TIME][:2]),
        int(row[Fields.TIME][3:5])
    )
    
    keywords_raw = row[Fields.KEYWORDS] if len(row) > Fields.KEYWORDS else ""
    keywords = filter_keywords([k.strip() for k in keywords_raw.split(",") if k.strip()])
    
    seats_raw = row[Fields.SEATS_AVAILABLE] if len(row) > Fields.SEATS_AVAILABLE else None
    seats = int(seats_raw) if seats_raw and str(seats_raw).isdigit() else None
    
    return Screening(
        id=row[Fields.ID],
        title=html.unescape(row[Fields.TITLE]),
        datetime=screening_dt,
        venue=Venue(
            id=row[Fields.VENUE_ID] if len(row) > Fields.VENUE_ID else "",
            name=row[Fields.VENUE_NAME] if len(row) > Fields.VENUE_NAME else "",
            short_name=normalise_venue_short(
                row[Fields.VENUE_SHORT] if len(row) > Fields.VENUE_SHORT else ""
            ),
        ),
        availability=Availability.from_code(row[Fields.AVAILABILITY]),
        sales_status=SalesStatus.from_code(row[Fields.SALES_STATUS]),
        seats_available=seats,
        keywords=keywords,
        min_price=row[Fields.MIN_PRICE] if len(row) > Fields.MIN_PRICE else None,
        max_price=row[Fields.MAX_PRICE] if len(row) > Fields.MAX_PRICE else None,
        article_url=row[Fields.ARTICLE_URL] if len(row) > Fields.ARTICLE_URL else None,
    )


# ============================================================
# COOKIE HANDLING (Firefox only)
# ============================================================

def get_firefox_profile_path() -> Optional[Path]:
    """Find the default Firefox profile directory."""
    import platform
    
    system = platform.system()
    home = Path.home()
    
    log.debug(f"Detecting Firefox profile for platform: {system}")
    
    if system == "Linux":
        firefox_dir = home / ".mozilla" / "firefox"
    elif system == "Darwin":
        firefox_dir = home / "Library" / "Application Support" / "Firefox" / "Profiles"
    elif system == "Windows":
        firefox_dir = Path(os.environ.get("APPDATA", "")) / "Mozilla" / "Firefox" / "Profiles"
    else:
        log.warning(f"Unknown platform '{system}'")
        return None
    
    if not firefox_dir.exists():
        log.warning(f"Firefox directory does not exist: {firefox_dir}")
        return None
    
    log.debug(f"Scanning {firefox_dir} for profiles...")
    
    profile_dirs = [
        item for item in firefox_dir.iterdir()
        if item.is_dir() and (".default" in item.name or "default" in item.name.lower())
    ]
    
    for profile in profile_dirs:
        if ".default-release" in profile.name:
            log.info(f"Selected Firefox profile: {profile.name}")
            return profile
    
    if profile_dirs:
        log.info(f"Selected Firefox profile: {profile_dirs[0].name}")
        return profile_dirs[0]
    
    log.warning("No Firefox profile directories found")
    return None


def load_cookies(domain: str = "whatson.bfi.org.uk") -> dict[str, str]:
    """Extract cookies for a domain from Firefox's cookie database."""
    log.info("=" * 60)
    log.info("COOKIE EXTRACTION: Firefox SQLite Database")
    log.info("=" * 60)
    
    profile_path = get_firefox_profile_path()
    if not profile_path:
        raise RuntimeError(
            "Could not find Firefox profile. Make sure Firefox is installed."
        )
    
    log.info(f"Using Firefox profile: {profile_path}")
    
    cookies_db = profile_path / "cookies.sqlite"
    if not cookies_db.exists():
        raise RuntimeError(f"Firefox cookies database not found at {cookies_db}")
    
    log.debug(f"Database size: {cookies_db.stat().st_size:,} bytes")
    
    temp_db = Path("/tmp/firefox_cookies_temp.sqlite")
    shutil.copy2(cookies_db, temp_db)
    
    try:
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # Build domain list including parent domain for two-part TLDs
        domains_to_search = [domain, f".{domain}"]
        parts = domain.split(".")
        two_part_tlds = {'org.uk', 'co.uk', 'ac.uk', 'gov.uk', 'com.au', 'co.nz', 'co.jp'}
        
        if len(parts) >= 2:
            potential_tld = ".".join(parts[-2:])
            if potential_tld in two_part_tlds and len(parts) > 3:
                parent_domain = ".".join(parts[-3:])
            elif len(parts) > 2:
                parent_domain = ".".join(parts[-2:])
            else:
                parent_domain = None
            
            if parent_domain and parent_domain != domain:
                domains_to_search.extend([f".{parent_domain}", parent_domain])
                log.debug(f"Also searching parent domain: {parent_domain}")
        
        log.debug(f"Searching domains: {domains_to_search}")
        
        placeholders = ",".join("?" * len(domains_to_search))
        cursor.execute(
            f"SELECT name, value, host FROM moz_cookies WHERE host IN ({placeholders})",
            domains_to_search
        )
        
        cookies = {}
        for name, value, host in cursor.fetchall():
            cookies[name] = value
            log.debug(f"  Cookie: {name} (from {host})")
        
        conn.close()
        
        if not cookies:
            raise RuntimeError(
                f"No cookies found for {domain}. "
                "Visit https://whatson.bfi.org.uk in Firefox first."
            )
        
        log.info(f"Found {len(cookies)} cookies")
        for key in ['cf_clearance', '__cf_bm']:
            log.info(f"  {'✓' if key in cookies else '✗'} {key}")
        
        return cookies
        
    finally:
        if temp_db.exists():
            temp_db.unlink()


def diagnose_all_bfi_cookies():
    """Search for ALL cookies containing 'bfi' in the domain."""
    profile_path = get_firefox_profile_path()
    if not profile_path:
        print("❌ Could not find Firefox profile")
        return
    
    cookies_db = profile_path / "cookies.sqlite"
    if not cookies_db.exists():
        print(f"❌ Cookies database not found: {cookies_db}")
        return
    
    temp_db = Path("/tmp/firefox_cookies_temp.sqlite")
    shutil.copy2(cookies_db, temp_db)
    
    try:
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name, value, host, isSecure FROM moz_cookies "
            "WHERE host LIKE '%bfi%' ORDER BY host, name"
        )
        rows = cursor.fetchall()
        conn.close()
        
        print(f"\n🔍 Found {len(rows)} cookies with 'bfi' in the domain:\n")
        
        if not rows:
            print("   No BFI cookies found. Close Firefox and retry.")
            return
        
        current_host = None
        for name, value, host, is_secure in rows:
            if host != current_host:
                current_host = host
                print(f"  📍 {host}")
            value_preview = value[:40] + "..." if len(value) > 40 else value
            print(f"     {'🔒' if is_secure else '  '} {name}: {value_preview}")
        
    finally:
        if temp_db.exists():
            temp_db.unlink()


# ============================================================
# HTTP REQUEST HANDLING
# ============================================================

def build_headers(referer: str) -> dict[str, str]:
    """Build request headers that match Firefox."""
    return {
        "Host": "whatson.bfi.org.uk",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:145.0) Gecko/20100101 Firefox/145.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    }


def fetch_single_day(date: dt.date, cookies: dict[str, str]) -> str:
    """Fetch screening data for a single day."""
    log.debug(f"Fetching {date}")
    
    referer_params = {
        "BOset::WScontent::SearchCriteria::search_from": date.isoformat(),
        "BOset::WScontent::SearchCriteria::search_to": date.isoformat(),
        "doWork::WScontent::search": "1",
        "BOparam::WScontent::search::article_search_id": ARTICLE_SEARCH_ID,
    }
    
    params = [
        ("BOset::WScontent::SearchCriteria::venue_filter", ""),
        ("BOset::WScontent::SearchCriteria::city_filter", ""),
        ("BOset::WScontent::SearchCriteria::month_filter", ""),
        ("BOset::WScontent::SearchCriteria::object_type_filter", ""),
        ("BOset::WScontent::SearchCriteria::category_filter", ""),
        ("BOset::WScontent::SearchCriteria::search_from", ""),
        ("BOset::WScontent::SearchCriteria::search_to", ""),
        ("doWork::WScontent::search", "1"),
        ("BOparam::WScontent::search::article_search_id", ARTICLE_SEARCH_ID),
        ("BOset::WScontent::SearchCriteria::search_criteria", ""),
        ("BOset::WScontent::SearchCriteria::search_from", date.isoformat()),
        ("BOset::WScontent::SearchCriteria::search_to", date.isoformat()),
    ]
    
    response = requests.get(
        BASE_URL,
        headers=build_headers(f"{BASE_URL}?{urlencode(referer_params)}"),
        cookies=cookies,
        params=params,
        timeout=30,
    )
    
    log.debug(f"Response: {response.status_code}, {len(response.text):,} bytes")
    
    if response.status_code == 403:
        raise RuntimeError(
            "🚫 Cloudflare returned 403. Cookies may have expired — "
            "visit https://whatson.bfi.org.uk in Firefox and retry."
        )
    
    response.raise_for_status()
    return response.text


# ============================================================
# HTML PARSING
# ============================================================

def extract_search_results(html_content: str) -> list[list]:
    """Extract the searchResults array from the page's JavaScript."""
    log.debug(f"Parsing HTML ({len(html_content):,} chars)")
    
    if "articleContext" not in html_content:
        if "cf-browser-verification" in html_content or "challenge-platform" in html_content:
            raise RuntimeError("Hit Cloudflare challenge. Cookies may be expired.")
        return []
    
    match = re.search(
        r"searchResults\s*:\s*\[\s*(.*?)\s*\],\s*searchFilters",
        html_content, re.DOTALL
    )
    
    if not match or not match.group(1).strip():
        return []
    
    json_text = f"[{match.group(1)}]".replace("'", '"')
    json_text = re.sub(r",\s*]", "]", json_text)
    json_text = re.sub(r",\s*}", "}", json_text)
    
    try:
        results = json.loads(json_text)
        log.info(f"Parsed {len(results)} screening rows")
        return results
    except json.JSONDecodeError as e:
        log.error(f"JSON parse error: {e}")
        return []


# ============================================================
# SCRAPING ORCHESTRATION
# ============================================================

def scrape_screenings(start_date: dt.date, days_ahead: int) -> list[Screening]:
    """Scrape screenings for a date range, day by day."""
    log.info("=" * 60)
    log.info(f"SCRAPING: {start_date} + {days_ahead} days")
    log.info("=" * 60)
    
    cookies = load_cookies()
    
    all_screenings: list[Screening] = []
    seen_ids: set[str] = set()
    
    for day_offset in range(days_ahead):
        current_date = start_date + dt.timedelta(days=day_offset)
        log.info(f"📅 {current_date.isoformat()} ({day_offset + 1}/{days_ahead})")
        
        html_content = fetch_single_day(current_date, cookies)
        rows = extract_search_results(html_content)
        
        day_count = 0
        for row in rows:
            try:
                screening = parse_screening(row)
                if screening.title.strip() == "Library Research Session":
                    continue
                if screening.id not in seen_ids:
                    seen_ids.add(screening.id)
                    all_screenings.append(screening)
                    day_count += 1
            except (IndexError, ValueError) as e:
                log.warning(f"  Parse error: {e}")
        
        log.info(f"  ✅ {day_count} screenings")
    
    all_screenings.sort(key=lambda s: s.datetime)
    log.info(f"Total: {len(all_screenings)} unique screenings")
    
    return all_screenings


# ============================================================
# FILTERING & PERSISTENCE
# ============================================================

def filter_screenings(
    screenings: list[Screening],
    venue: Optional[str] = None,
    available_only: bool = False,
    title_contains: Optional[str] = None,
    keyword: Optional[str] = None,
) -> list[Screening]:
    """Filter screenings by various criteria."""
    results = screenings
    
    if venue:
        venue_lower = venue.lower()
        results = [s for s in results if venue_lower in s.venue.short_name.lower() or venue_lower in s.venue.name.lower()]
    if available_only:
        results = [s for s in results if s.is_available]
    if title_contains:
        title_lower = title_contains.lower()
        results = [s for s in results if title_lower in s.title.lower()]
    if keyword:
        keyword_lower = keyword.lower()
        results = [s for s in results if any(keyword_lower in k.lower() for k in s.keywords)]
    
    return results


def save_screenings(screenings: list[Screening], path: Path = DATA_PATH):
    """Save screenings to JSON file."""
    data = {
        "fetched_at": dt.datetime.now().isoformat(),
        "count": len(screenings),
        "screenings": [s.to_dict() for s in screenings],
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Saved {len(screenings)} screenings to {path}")


def load_screenings(path: Path = DATA_PATH) -> list[Screening]:
    """Load screenings from JSON file."""
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return [Screening.from_dict(s) for s in data.get("screenings", [])]


# ============================================================
# HTML TEMPLATE (unified for serve & static)
# ============================================================

HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>BFI Screenings</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <link rel="apple-touch-icon" href="https://www.bfi.org.uk/sites/bfi.org.uk/themes/flavor/images/apple-touch-icon.png">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&family=Inter:wght@400;500&display=swap');
        * { box-sizing: border-box; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: #faf9f6; color: #1a1a1a;
            margin: 0; padding: 0; line-height: 1.6; font-size: 15px;
        }
        header {
            border-bottom: 1px solid #d4d0c8;
            padding: 2.5rem 2rem 2rem;
            background: #fff;
            position: sticky; top: 0; z-index: 100;
        }
        header h1 {
            font-family: 'Libre Baskerville', Georgia, serif;
            margin: 0; font-weight: 400; font-size: 1.75rem;
        }
        header p { margin: 0.5rem 0 0; color: #666; font-size: 0.875rem; }
        .generated-date { font-size: 0.75rem; color: #999; margin-top: 0.25rem; }
        main { max-width: 720px; margin: 0 auto; padding: 1.5rem 2rem 4rem; }
        .filters {
            background: #fff; border: 1px solid #e5e2db;
            padding: 1rem 1.25rem; margin-bottom: 2rem;
        }
        .filters-row { display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center; }
        .filters input[type="text"], .filters select {
            background: #faf9f6; border: 1px solid #d4d0c8;
            padding: 0.5rem 0.75rem; font-size: 0.8125rem;
            font-family: inherit; flex: 1; min-width: 120px;
        }
        .filters input:focus, .filters select:focus { outline: none; border-color: #1a1a1a; }
        .filters label {
            display: flex; align-items: center; gap: 0.4rem;
            font-size: 0.8125rem; color: #666; white-space: nowrap;
        }
        .date-group { margin-top: 2.5rem; }
        .date-group:first-of-type { margin-top: 0; }
        .date-group.hidden { display: none; }
        .date-header {
            font-family: 'Libre Baskerville', Georgia, serif;
            font-size: 1.3rem; font-weight: 400; font-style: italic;
            margin: 0 0 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid #d4d0c8;
        }
        .screening {
            padding: 0.875rem 0; border-bottom: 1px solid #e5e2db;
            display: grid; grid-template-columns: 3.5rem 1fr auto;
            gap: 1rem; align-items: baseline;
        }
        .screening.hidden { display: none; }
        .screening:last-child:not(.hidden) { border-bottom: none; }
        .screening-time { font-variant-numeric: tabular-nums; font-size: 0.875rem; color: #666; }
        .screening-title { font-family: 'Libre Baskerville', Georgia, serif; font-size: 1.1rem; line-height: 1.4; }
        .screening-title a { color: inherit; text-decoration: none; }
        .screening-title a:hover { text-decoration: underline; }
        .screening-meta { font-size: 0.8125rem; color: #888; margin-top: 0.2rem; font-weight: 700; }
        .screening-lb {
            font-size: 0.6rem; color: #303030; margin-top: 0.2rem;
            font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em;
        }
        .screening-lb a { color: inherit; text-decoration: none; }
        .screening-lb a:hover { text-decoration: underline; }
        .screening-keywords { display: flex; flex-wrap: wrap; gap: 0.3rem; margin-top: 0.35rem; }
        .keyword-tag {
            background: transparent; color: #888; font-size: 0.6875rem;
            text-transform: uppercase; letter-spacing: 0.04em;
        }
        .keyword-tag::after { content: ' · '; color: #ccc; }
        .keyword-tag:last-child::after { content: ''; }
        .screening-availability {
            font-size: 0.75rem; text-align: right; white-space: nowrap;
            text-transform: uppercase; letter-spacing: 0.03em;
        }
        .avail-excellent { color: #2e7d32; }
        .avail-good { color: #558b2f; }
        .avail-limited { color: #e75d13; }
        .avail-sold_out { color: #c04401; }
        .empty-state {
            text-align: center; padding: 4rem 2rem;
            color: #888; font-style: italic; display: none;
        }
        .empty-state.visible { display: block; }
        @media (max-width: 600px) {
            header { padding: 1.5rem 1rem; }
            main { padding: 1rem; }
            .screening { grid-template-columns: 1fr; gap: 0.25rem; }
            .screening-time { font-size: 0.8125rem; }
            .screening-availability { text-align: left; margin-top: 0.25rem; }
            .filters input, .filters select { min-width: 100%; }
        }
    </style>
</head>
<body>
    <header>
        <h1>BFI Southbank Screenings</h1>
        <p class="generated-date">Last updated {{ generated_at }}</p>
    </header>
    <main>
        <div class="filters">
            <div class="filters-row">
                <input type="text" id="filter-title" placeholder="Search titles…">
                <input type="text" id="filter-keyword" placeholder="Keyword…">
                <select id="filter-venue">
                    <option value="">All venues</option>
                    {% for v in venues %}<option value="{{ v }}">{{ v }}</option>{% endfor %}
                </select>
                <label><input type="checkbox" id="filter-available"> Available only</label>
            </div>
        </div>
        <div id="screenings-container">
            {% for date, items in grouped_screenings.items() %}
            <div class="date-group" data-date="{{ date }}">
                <h2 class="date-header">{{ date }}</h2>
                {% for s in items %}
                <div class="screening"
                     data-title="{{ s.title|lower }}"
                     data-venue="{{ s.venue.short_name }}"
                     data-keywords="{{ s.keywords|join(',')|lower }}"
                     data-available="{{ 'yes' if s.is_available else 'no' }}"
                     data-datetime="{{ s.datetime.isoformat() }}">                    <div class="screening-time">{{ s.time_str }}</div>
                    <div class="screening-info">
                        <div class="screening-title">
                            <a href="{{ s.booking_url }}" target="_blank">{{ s.title }}</a>
                        </div>
                        <div class="screening-meta">{{ s.venue.short_name }}</div>
                        {% if s.keywords %}
                        <div class="screening-keywords">
                            {% for kw in s.keywords %}<span class="keyword-tag">{{ kw }}</span>{% endfor %}
                        </div>
                        {% endif %}
                        <div class="screening-lb">
                            <a href="https://letterboxd.com/search/{{ s.title }}" target="_blank">LETTERBOXD</a>
                        </div>
                    </div>
                    <div class="screening-availability avail-{{ s.availability.name.lower() }}">
                        {{ s.availability.display }}
                    </div>
                </div>
                {% endfor %}
            </div>
            {% endfor %}
        </div>
        <div class="empty-state" id="empty-state"><p>No screenings match your filters.</p></div>
    </main>
    <script>
        const screenings = document.querySelectorAll('.screening');
        const dateGroups = document.querySelectorAll('.date-group');
        const countDisplay = document.getElementById('count-display');
        const emptyState = document.getElementById('empty-state');
        const filterTitle = document.getElementById('filter-title');
        const filterKeyword = document.getElementById('filter-keyword');
        const filterVenue = document.getElementById('filter-venue');
        const filterAvailable = document.getElementById('filter-available');
        
        function hidePastScreenings() {
            const now = new Date();
            screenings.forEach(el => {
                const screeningDate = new Date(el.dataset.datetime);
                if (screeningDate < now) {
                    el.classList.add("hidden");
                }
            });
 
            // Hide any date-group that becomes empty
            dateGroups.forEach(group => {
                const hasVisible = group.querySelector(".screening:not(.hidden)");
                group.classList.toggle("hidden", !hasVisible);
            });
        }

        function applyFilters() {
            const titleQ = filterTitle.value.toLowerCase().trim();
            const keywordQ = filterKeyword.value.toLowerCase().trim();
            const venueQ = filterVenue.value;
            const availOnly = filterAvailable.checked;
            let count = 0;
            
            screenings.forEach(s => {
                let show = true;
                if (titleQ && !s.dataset.title.includes(titleQ)) show = false;
                if (keywordQ && !s.dataset.keywords.includes(keywordQ)) show = false;
                if (venueQ && s.dataset.venue !== venueQ) show = false;
                if (availOnly && s.dataset.available !== 'yes') show = false;
                s.classList.toggle('hidden', !show);
                if (show) count++;
            });
            
            dateGroups.forEach(g => {
                g.classList.toggle('hidden', !g.querySelectorAll('.screening:not(.hidden)').length);
            });
            
            countDisplay.textContent = count + ' screening' + (count !== 1 ? 's' : '');
            emptyState.classList.toggle('visible', count === 0);
        }
        
        filterTitle.addEventListener('input', applyFilters);
        filterKeyword.addEventListener('input', applyFilters);
        filterVenue.addEventListener('change', applyFilters);
        filterAvailable.addEventListener('change', applyFilters);
        hidePastScreenings();
        applyFilters();
    </script>
</body>
</html>
"""


def render_html(screenings: list[Screening]) -> str:
    """Render screenings to HTML using the unified template."""
    from jinja2 import Template
    
    grouped: dict[str, list] = {}
    for s in screenings:
        date_key = s.datetime.strftime("%A %d %B")
        grouped.setdefault(date_key, []).append(s)
    
    venues = sorted(set(s.venue.short_name for s in screenings if s.venue.short_name))
    
    return Template(HTML_TEMPLATE).render(
        grouped_screenings=grouped,
        venues=venues,
        generated_at=dt.datetime.now().strftime("%d %B %Y at %H:%M"),
    )


# ============================================================
# FLASK WEB SERVER
# ============================================================

app = Flask(__name__)

@app.route("/")
def index():
    screenings = load_screenings()
    now = dt.datetime.now()
    screenings = [s for s in screenings if s.datetime >= now]
    return render_html(screenings)


# ============================================================
# CLI COMMANDS
# ============================================================

def cmd_fetch(args):
    """Fetch screening data and save to JSON."""
    global log
    log = setup_logging(verbose=args.verbose, log_file=LOG_PATH)
    
    screenings = scrape_screenings(dt.date.today(), args.days)
    save_screenings(screenings)
    
    print(f"\n✅ Saved {len(screenings)} screenings to {DATA_PATH}")


def cmd_serve(args):
    """Start the web server."""
    global log
    log = setup_logging(verbose=args.verbose, log_file=LOG_PATH)
    
    if args.refresh:
        screenings = scrape_screenings(dt.date.today(), DEFAULT_DAYS_AHEAD)
        save_screenings(screenings)
    
    print(f"🌐 Serving on http://localhost:{args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)


def cmd_list(args):
    """List screenings in the terminal."""
    global log
    log = setup_logging(verbose=args.verbose, log_file=LOG_PATH)
    
    if args.refresh or not DATA_PATH.exists():
        screenings = scrape_screenings(dt.date.today(), args.days)
        save_screenings(screenings)
    else:
        screenings = load_screenings()
    
    filtered = filter_screenings(
        screenings, venue=args.venue, available_only=args.available_only,
        title_contains=args.title, keyword=args.keyword,
    )
    
    if not filtered:
        print("No screenings found matching your criteria.")
        return
    
    current_date = None
    for s in filtered:
        date_str = s.datetime.strftime("%A %d %B")
        if date_str != current_date:
            current_date = date_str
            print(f"\n{date_str}")
            print("─" * len(date_str))
        print(f"  {s.time_str}  {s.availability.emoji} {s.availability.display:12}  {s.venue.short_name:5}  {s.title}")
    
    print(f"\n{len(filtered)} screenings found.")


def cmd_cookies(args):
    """Test Firefox cookie extraction."""
    global log
    log = setup_logging(verbose=args.verbose, log_file=LOG_PATH)
    
    if args.diagnose:
        diagnose_all_bfi_cookies()
        return
    
    try:
        cookies = load_cookies()
        print(f"✅ Found {len(cookies)} cookies")
        for key in ['cf_clearance', '__cf_bm']:
            if key in cookies:
                print(f"  {key}: {cookies[key][:50]}...")
    except RuntimeError as e:
        print(f"❌ {e}")
        print("\n💡 TIP: Close Firefox completely, then retry. Or use --diagnose")
        sys.exit(1)


def cmd_static(args):
    """Generate a static HTML file."""
    global log
    log = setup_logging(verbose=args.verbose, log_file=LOG_PATH)
    
    if args.refresh or not DATA_PATH.exists():
        screenings = scrape_screenings(dt.date.today(), args.days)
        save_screenings(screenings)
    else:
        screenings = load_screenings()
    
    now = dt.datetime.now()
    screenings = [s for s in screenings if s.datetime >= now]
    
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html(screenings), encoding="utf-8")
    
    print(f"✅ Generated {output_path} with {len(screenings)} screenings")


def main():
    parser = argparse.ArgumentParser(description="BFI Southbank screening scraper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # fetch
    p = subparsers.add_parser("fetch", help="Fetch screening data")
    p.add_argument("--days", type=int, default=DEFAULT_DAYS_AHEAD)
    p.add_argument("--verbose", "-v", action="store_true")
    p.set_defaults(func=cmd_fetch)
    
    # serve
    p = subparsers.add_parser("serve", help="Start web server")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.set_defaults(func=cmd_serve)
    
    # list
    p = subparsers.add_parser("list", help="List screenings in terminal")
    p.add_argument("--days", type=int, default=DEFAULT_DAYS_AHEAD)
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--venue")
    p.add_argument("--title")
    p.add_argument("--keyword")
    p.add_argument("--available-only", action="store_true")
    p.set_defaults(func=cmd_list)
    
    # cookies
    p = subparsers.add_parser("cookies", help="Test cookie extraction")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--diagnose", action="store_true")
    p.set_defaults(func=cmd_cookies)
    
    # static
    p = subparsers.add_parser("static", help="Generate static HTML")
    p.add_argument("--output", "-o", default="bfi.html")
    p.add_argument("--days", type=int, default=DEFAULT_DAYS_AHEAD)
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.set_defaults(func=cmd_static)
    
    args = parser.parse_args()
    
    try:
        args.func(args)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)


if __name__ == "__main__":
    main()