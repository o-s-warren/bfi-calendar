# BFI Southbank Screening Scraper

A Python CLI tool that scrapes film screening data from BFI Southbank's ticketing system and presents it in a clean, filterable interface.

## Features

- Scrapes screening times, venues, ticket status, and keywords
- Web interface with real-time filtering by title, keyword, venue, and availability
- Static HTML export for hosting on Netlify or similar
- Terminal listing with filters
- Links to Letterboxd for each film

## Prerequisites

- **Python 3.10+**
- **Firefox** - Required for cookie extraction (the BFI site uses Cloudflare protection)

## Setup

```bash
# Create and activate virtual environment
python3 -m venv bfiCalender
source bfiCalender/bin/activate

# Install dependencies
pip install requests flask jinja2
```

## First-Time Setup: Cloudflare Cookies

The BFI website uses Cloudflare bot protection. Before scraping:

1. Open Firefox
2. Visit https://whatson.bfi.org.uk
3. Complete any Cloudflare challenge if prompted
4. Close Firefox completely (important - the cookie database is locked while Firefox runs)

The scraper will extract the necessary cookies from Firefox's profile.

## Usage

### Fetch screening data

```bash
python bfi_calendar.py fetch --days 28 --verbose
```

Scrapes screenings for the next 28 days and saves to `bfi_screenings.json`.

### Run local web server

```bash
python bfi_calendar.py serve --port 8000
```

Serves a filterable web interface at http://localhost:8000

Options:
- `--refresh` - Re-scrape data before serving
- `--debug` - Enable Flask debug mode

### Generate static HTML

```bash
python bfi_calendar.py static --days 28 --output index.html
```

Creates a self-contained HTML file for static hosting.

### List in terminal

```bash
python bfi_calendar.py list --days 7
python bfi_calendar.py list --venue NFT1 --available-only
python bfi_calendar.py list --title "cassavetes" --keyword "35mm"
```

### Diagnose cookie issues

```bash
python bfi_calendar.py cookies           # Test extraction
python bfi_calendar.py cookies --diagnose  # List all BFI cookies
```

## Troubleshooting

**"No cookies found" or 403 errors:**
- Close Firefox completely and retry
- Re-visit https://whatson.bfi.org.uk in Firefox to refresh cookies
- Use `--diagnose` to see what cookies exist

**"Could not find Firefox profile":**
- Ensure Firefox is installed
- On macOS, profiles are in `~/Library/Application Support/Firefox/Profiles/`

## Deployment

For Netlify or similar static hosting:

```bash
python bfi_calendar.py static --days 28 --output netlify/index.html
```

Currently available at https://bfi-southbank-screenings.netlify.app/