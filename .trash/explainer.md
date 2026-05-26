# CMM Repository Explainer

## A Complete Guide for Anyone — No Programming Experience Required

---

## Part 1: What Is This Project?

CMM stands for **China Macro Monitor**. It is a collection of computer programs that automatically gather information about China's economy, government policies, financial markets, trade activity, flight traffic, and ship movements. All of this information is saved into a local database on your computer, and a web-based dashboard lets you browse through it in a visual way.

Think of it like building your own personal news and data aggregator, specifically focused on China's economy and government. Instead of you going to dozens of different websites every day, these programs go to those websites for you, grab the latest information, and neatly organize it in one place.

---

## Part 2: Essential Vocabulary

Before we look at any files, here are the basic terms you need to understand. Every term used later in this document will have already been explained here.

### What is a "repository" (repo)?

A **repository** is simply a folder on your computer that contains all the files for a project. It also tracks the history of every change ever made to those files using a system called **Git** (explained next). When people say "the repo," they mean this folder and everything inside it.

### What is Git?

**Git** is a tool that records a history of changes to files. Every time someone saves a meaningful change, they create a "snapshot" called a **commit**. This means you can always go back to an earlier version if something breaks. Git is used by virtually every software project in the world.

### What is a "script"?

A **script** is a text file containing instructions written in a programming language. When you "run" a script, the computer reads those instructions from top to bottom and carries them out. In this project, all scripts are written in **Python**, a popular programming language. Python files always end in `.py`.

### What is a "module" and a "package"?

A **module** is a single Python file (like `parser.py`). A **package** is a folder that contains multiple modules (Python files) that work together. To mark a folder as a Python package, it must contain a special file called `__init__.py` (this file can be empty; it just signals "this folder is a package").

In this project, there are two main packages:
- `customs_scraper/` — the China Customs export data scraper
- `policy_monitor/` — the policy, news, and financial data monitor

### What is a "database"?

A **database** is an organized collection of information stored on your computer. Think of it like a spreadsheet with multiple tabs, where each tab is a **table**, and each table has rows and columns. This project uses **SQLite**, which stores the entire database in a single file (ending in `.db`). No special database software needs to be installed — Python can read and write SQLite files directly.

### What is "scraping"?

**Scraping** (or "web scraping") means using a program to automatically visit a website and extract information from it, the same way you would if you opened the page in a browser and copied data by hand — except the program does it much faster and can repeat the process thousands of times.

### What is an "API"?

An **API** (Application Programming Interface) is a structured way for programs to request data from a service. Instead of loading a visual webpage meant for human eyes, an API sends back raw data in a format that programs can easily read. Many of the data sources in this project provide APIs.

### What is "RSS"?

**RSS** (Really Simple Syndication) is a standard format that websites use to publish a list of their latest articles. An RSS "feed" is like a machine-readable table of contents that updates automatically. This project reads RSS feeds from Chinese news outlets to track policy announcements.

### What is a "dependency"?

A **dependency** is an external package (a collection of code written by someone else) that this project needs in order to work. For example, this project depends on `flask` (for building web pages), `feedparser` (for reading RSS feeds), and `requests` (for downloading web pages). Dependencies are listed in a requirements file so they can be installed easily.

### What is an "environment variable"?

An **environment variable** is a setting stored outside of the code itself, usually containing sensitive information like passwords or API keys. They are kept in a file called `.env` on your computer. The code reads these variables when it runs, so the passwords never need to be written directly into the code.

### What are "HS codes"?

**HS codes** (Harmonized System codes) are internationally standardized numbers used to classify traded products. For example, HS code `84713000` refers to "portable digital automatic data processing machines" (laptops). China Customs uses 8-digit HS codes to categorize every product that is exported or imported.

### What is "launchd"?

**launchd** is a macOS system tool that can automatically run programs on a schedule, similar to an alarm clock for software. You create a configuration file (called a "plist") that tells macOS: "Run this program every day at 3:00 AM" or "Keep this program running at all times."

### What is a "dashboard"?

A **dashboard** is a web page that displays data visually — charts, tables, maps — so you can quickly understand what is happening. In this project, the dashboard runs locally on your computer (not on the internet) and shows all the collected data in one place.

---

## Part 3: The Top-Level Files

These files sit at the root of the repository (the outermost folder). They configure the project as a whole.

### `pyproject.toml`

This is the project's **identity card**. It tells Python: "This project is called `cmm`, it is version `0.1.0`, and it requires Python version 3.11 or newer." It also lists which folders contain code (`customs_scraper` and `policy_monitor`) and configures the testing tool to look for tests in the `tests/` folder.

### `requirements.txt`

This file lists all **dependencies** — the external packages the project needs. It is organized into three sections:

1. **Customs Scraper dependencies**: `scrapling` (for browser-based scraping), `apscheduler` (for scheduling tasks), and `python-dotenv` (for reading `.env` files).
2. **Policy Monitor dependencies**: `feedparser` (for RSS), `requests` (for downloading web pages), `pyyaml` (for reading configuration files in YAML format), `akshare` (for Chinese financial data), `flask` (for the web dashboard), `openpyxl` (for reading Excel files), `global-macro-data` (for macroeconomic data), and `websockets` (for real-time ship tracking).
3. **Testing dependencies**: `pytest` (for running automated tests) and `lxml` (for parsing HTML).

To install all dependencies, you would run: `pip install -r requirements.txt`

### `.env.example`

This is a **template** for the environment variables file. You copy it to `.env` and fill in your own values. It contains placeholders for:
- Database file location
- HTTP proxy address (needed to access the Chinese customs website from outside China)
- Scraping speed and retry settings
- The Chinese Customs website address
- Scheduling settings (which day and time to run the scraper)
- Login credentials for flight tracking (OpenSky Network) and ship tracking (AISStream, AISHub)

The actual `.env` file (with your real passwords) is never stored in Git — it is listed in `.gitignore` to prevent accidental sharing.

### `.gitignore`

This file tells Git which files to **ignore** — that is, which files should never be tracked or shared. It ignores:
- `__pycache__/` and `.pyc` files (automatically generated by Python; not useful to share)
- `.env` (contains passwords)
- `.DS_Store` (macOS system files)
- `*.db` (database files, which can be very large)
- `customs_scraper.log` (log files that record what happened during scraping)
- `.pytest_cache/` (test caching)
- `data/feeds.db`, `data/logs/`, `data/config.json` (runtime data)
- `.idea/` (PyCharm editor settings)

### `CLAUDE.md`

This file contains instructions for an AI assistant (Claude) that helps with development. It describes coding standards, workflow rules, and project conventions. It is not part of the running software — it guides the development process.

### `README.md`

The project's front page documentation. Currently a placeholder ("TO BE UPDATED").

### `workplan-4-week-mvp.md`

A detailed 4-week work plan for building a "Minimum Viable Product" (the simplest useful version of the project). It outlines 20 days of tasks covering architecture, data ingestion, analytics, and dashboard building. This is a planning document, not running code.

---

## Part 4: The Run Scripts (Root Level)

At the top level of the repo, there are six Python scripts that start with `run_`. These are **entry points** — the scripts you actually execute to start different parts of the system. They are deliberately simple: each one just imports the real logic from the packages below and calls it. This keeps the "how to start the program" separate from "what the program does."

### `run_dashboard.py`

Starts the web dashboard. When you run this script, it launches a local web server at `http://127.0.0.1:5001` (your computer, port 5001). You then open this address in a web browser to see the dashboard.

### `run_fetch_batch.py`

Runs the **batch data pipeline**. "Batch" means it collects a large amount of data all at once (as opposed to continuously). This fetches financial market data, dissent event data, Bruegel economic data, macroeconomic data, academic articles, and customs export data — then stores everything in the database.

### `run_fetch_realtime.py`

Starts the **real-time data fetcher**. Unlike the batch pipeline that runs once and stops, this script runs continuously in the background. It repeatedly checks for new flight positions (every 1-15 minutes), ship positions (every ~60 seconds), and RSS news feeds (every hour).

### `run_fetch_all.py`

An older script that combines batch and news fetching. It is **deprecated** (meaning it still works but is no longer the recommended way to do things). The project now uses `run_fetch_batch.py` and `run_fetch_realtime.py` separately instead.

### `run_fetch_news.py`

Fetches RSS news feeds once and stores them. Also deprecated in favor of `run_fetch_realtime.py`, which handles news as part of its continuous loop.

### `run_fetch_macro.py`

Fetches macroeconomic data with a random delay (to avoid always hitting data sources at the same time). Also deprecated in favor of `run_fetch_batch.py`.

---

## Part 5: The `data/` Folder

This folder contains data files that the programs use or produce.

### `data/feeds.db` (not stored in Git)

The central **SQLite database** file. All collected data — news articles, financial indicators, ship positions, flight positions, dissent events, economic data — is stored here. This file can grow to hundreds of megabytes as data accumulates.

### `data/config.json`

A configuration file containing API credentials (login information for the OpenSky flight tracking API and the AISStream ship tracking API). The programs read this file to know how to authenticate with these services.

### `data/china_provinces.json`

A large file containing geographic data for all of China's provinces. Used by the dashboard to display maps.

### `data/china_prefectures.json`

A large file containing geographic data for China's prefectures (smaller administrative divisions within provinces). Also used for map displays.

### `data/logs/` (not stored in Git)

A folder that contains **log files**. A log file is a record of what a program did, including any errors it encountered. Each log file is named with the date and the type of task that produced it (for example, `news_2026-04-13.log` records the news fetching activity on April 13, 2026). Logs are useful for diagnosing problems.

---

## Part 6: The `customs_scraper/` Package — China Customs Export Data

This package contains all the code for scraping export trade data from China's General Administration of Customs website (`stats.customs.gov.cn`). The idea is to automatically download monthly export statistics — how much of each product China exported to each country, and at what value.

### `customs_scraper/__init__.py`

An empty file that marks the `customs_scraper/` folder as a Python package. Without this file, Python would not recognize the folder as a package and would not be able to import code from it.

### `customs_scraper/config.py`

The **configuration center** for the customs scraper. It reads settings from environment variables (your `.env` file) and makes them available to all other files in the package. The settings include:

- **DB_PATH**: Where the database file is located. Defaults to `data/feeds.db`.
- **CUSTOMS_PROXY_URL**: The address of an HTTP proxy with a Chinese IP address. The customs website blocks access from outside China, so a proxy (a middleman server located in China) is needed to reach it.
- **SCRAPE_DELAY_SECONDS**: How long to wait between requests (2 seconds by default), to avoid overwhelming the website.
- **SCRAPE_MAX_RETRIES**: How many times to retry a failed request (5 times by default).
- **SCRAPE_RETRY_BASE_SECONDS**: The starting wait time when retrying. Each retry waits longer than the last (this is called "exponential backoff").
- **SCRAPE_HEADLESS**: Whether to run the browser invisibly (without showing a window). Normally `true`.
- **SCRAPE_USE_DYNAMIC**: Whether to use a real browser engine (needed because the customs website uses JavaScript to load data dynamically).
- **BASE_URL**: The customs website address (`http://stats.customs.gov.cn`).
- **QUERY_ENDPOINT**: The specific page on the website where you submit export data queries.
- **Scheduler settings**: Which day of the month, what hour, and what time zone to run the automatic monthly scrape.

### `customs_scraper/db.py`

The **database layer** for customs data. This file handles all interactions with the SQLite database. It defines three tables:

1. **`exports`**: The main data table. Each row represents one export record: a specific product (identified by its 8-digit HS code), exported to a specific country, in a specific year and month, with values in USD and CNY and a quantity. The combination of year + month + HS code + country is unique — you cannot have duplicate rows for the same product-country-month.

2. **`scrape_runs`**: Tracks each scraping session. When the program starts scraping, it creates a new "run" record with a unique ID, records when it started, and later updates it with when it finished, whether it succeeded or failed, and how many rows were processed.

3. **`scrape_checkpoints`**: Tracks progress within a run. Since there are approximately 9,000 different HS codes, scraping all of them takes a long time. If the program crashes halfway through, checkpoints let it resume where it left off instead of starting over.

The file provides functions to:
- Create the database tables (`init_db`)
- Open a connection to the database (`get_conn`)
- Start and finish tracking a scraping run (`start_run`, `finish_run`)
- Insert or update export data rows (`upsert_export_rows`) — "upsert" means "insert if new, update if already exists"
- Record and read checkpoints (`checkpoint_done`, `get_completed_hs_codes`)

### `customs_scraper/hs_codes.py`

A simple file that **loads the list of HS codes** from a CSV file (`data/hs8_codes.csv`). Each HS code has an 8-digit number and a text description of the product. The scraper iterates through this list, querying the customs website for each code one by one. If the CSV file does not exist, the program raises an error with instructions on how to create it.

### `customs_scraper/countries.py`

Similar to `hs_codes.py`, but for **country codes**. It loads the list of destination countries from `data/countries.csv`. Each country has a numeric code and a name. These are used when the customs website requires you to specify which destination country you want data for.

### `customs_scraper/bootstrap.py`

A large and sophisticated file that **fetches the HS code list and country list** from the customs website itself. You run this once before your first full scrape, to populate the `data/hs8_codes.csv` and `data/countries.csv` files.

It tries multiple strategies, in order:
1. **Local file**: If you have a downloaded Excel tariff schedule, it can parse HS codes from that.
2. **API probe**: It tries various possible API addresses on the customs website, seeing if any return structured data.
3. **Page HTML scan**: It loads the customs website query page and searches through the source code for embedded lists of codes.
4. **Playwright DOM extraction**: It opens a real browser, waits for the page to load, and extracts codes from interactive dropdown menus.

If all strategies fail, it provides a helpful error message with manual alternatives.

### `customs_scraper/fetcher.py`

The **web fetching layer**. This file wraps the `scrapling` library (which controls a real web browser) and adds:
- Retry logic with exponential backoff (if a request fails, wait longer before trying again)
- Proxy support (routing requests through a Chinese IP)
- Session management (keeping a browser window open across many requests to avoid the overhead of starting a new browser each time)

It provides two main methods:
- `fetch()`: Uses a browser engine for pages that require JavaScript
- `fetch_static()`: Uses simple HTTP requests for pages that are plain HTML

### `customs_scraper/parser.py`

The **HTML parser**. After the fetcher downloads a web page from the customs website, this file extracts the actual data from it. It looks for a data table in the HTML and reads each row, mapping columns to fields like HS code, country code, export value in USD, export value in CNY, quantity, and unit.

The selectors (the patterns used to find elements on the page) are currently **stubs** — placeholders that need to be updated after someone manually inspects the real customs website. The file includes detailed instructions on how to do this inspection.

It also includes functions to:
- Check if there is a "next page" button (results may span multiple pages)
- Extract the total number of results shown on the page

### `customs_scraper/page_actions.py`

The **browser automation layer**. These are functions that interact with the customs website as if you were clicking buttons and typing into forms. They:
- Fill in the year and month fields on the query form
- Enter an HS code into the search box
- Click the "Submit" button
- Wait for the results table to appear
- Click the "Next Page" button to see more results

Like the parser, these are currently **stubs** that need to be updated after manual inspection of the real website.

### `customs_scraper/orchestrator.py`

The **conductor** that coordinates the entire scraping process. For a given year and month:
1. It loads the list of all ~9,000 HS codes
2. If resuming a previous run, it checks which codes were already completed and skips them
3. It opens a browser session
4. For each HS code, it submits a query, reads all pages of results, and saves the data to the database
5. After each HS code is completed, it records a checkpoint
6. At the end, it records whether the run succeeded, partially succeeded, or failed

### `customs_scraper/scheduler.py`

The **automatic scheduling system**. If you want the scraper to run automatically every month without you doing anything, this file sets up a recurring job using APScheduler. By default, it runs on the 15th of each month at 8:00 AM (Shanghai time), because that is typically when China Customs publishes the previous month's data. It scrapes the previous month's data (for example, running on February 15th would scrape January's data).

### `customs_scraper/debug.py`

A **diagnostic tool**. When you run the scraper with the `--debug-browser` flag, this file opens a visible (non-headless) browser window pointing at the customs website. You can then manually inspect the page, look at network requests in the browser's developer tools, and figure out the correct selectors for `parser.py` and `page_actions.py`. It can also save the current page's HTML to a test fixture file.

### `customs_scraper/main.py`

The **command-line entry point** for the customs scraper. It parses command-line arguments and routes to the appropriate action:
- No arguments: scrape the previous month's data
- `--year 2024 --month 1`: scrape a specific month
- `--schedule`: start the automatic monthly scheduler (runs in the background indefinitely)
- `--resume <run_id>`: resume a failed or interrupted scraping run
- `--debug-browser`: open a browser for manual site inspection
- `--bootstrap-hs-codes`: fetch the lists of HS codes and countries
- `--from-xls <file>`: parse HS codes from a local Excel file

---

## Part 7: The `policy_monitor/` Package — News, Financial, and Policy Data

This is the larger of the two main packages. It contains code for fetching data from many different sources, storing it, and presenting it through a web dashboard.

### `policy_monitor/__init__.py`

Empty file that marks this folder as a Python package.

### `policy_monitor/storage.py`

The **core database module** for the policy monitor. It defines the central database file location (`data/feeds.db`) and creates three tables:

1. **`items`**: Stores news articles from RSS feeds. Each item has a source name, category, title, link, publication date, summary text, and the time it was fetched.

2. **`fetch_log`**: Records every time the program attempted to fetch a feed — whether it succeeded, any error messages, and how many items were returned.

3. **`batch_runs`**: Tracks batch pipeline runs — when they started, when they completed, which data sources were attempted, which succeeded, and which failed.

It provides functions to connect to the database, store feed results, retrieve recent items, count total items, and get fetch statistics.

### `policy_monitor/sources/registry.yaml`

The **master directory** of Chinese government information sources. This is a large YAML-format file (a human-readable configuration format) that catalogs over 60 Chinese government bodies, news outlets, and data sources, organized into categories:

1. **Central Government** (4 sources): State Council, National People's Congress, CPPCC, NDRC
2. **Specialized Ministries** (20 sources): Foreign Affairs, Finance, Commerce, Industry, Education, Science, Environment, and many more
3. **Regulatory Bodies** (17 sources): People's Bank of China, Securities Commission, Customs Administration, Statistics Bureau, and more
4. **Party and Discipline Bodies** (2 sources): CCDI (anti-corruption), CPC Central Committee
5. **Judiciary** (4 sources): Supreme Court, Supreme Procuratorate, national law databases
6. **State Media and News** (12 sources): Xinhua, People's Daily, CCTV, Global Times, Caixin, and more
7. **Social Media / WeChat Accounts** (8 accounts): Official government WeChat accounts
8. **Open Legal and Data Resources** (8 resources): Law databases, open data portals
9. **Direct RSS Feeds** (11 feeds): Verified RSS feeds accessible from outside China without special tools

Each source entry includes its official name (in English and Chinese), website URL, category tag, content types it publishes, and optionally an RSSHub route (a tool that converts non-RSS websites into RSS feeds).

### `policy_monitor/sources/loader.py`

Functions that **read the registry** and return filtered lists of sources. It can return:
- All sources across all categories
- Only sources with RSSHub routes
- Only direct RSS feeds (that work from anywhere without special tools)
- All fetchable feeds (direct + RSSHub combined)
- Sources filtered by category or content type
- WeChat accounts

### `policy_monitor/sources/validate.py`

A **health check tool** that visits every URL in the registry to see if it is reachable. It tests all sources concurrently (many at the same time, for speed) and prints a report showing which are accessible and which are not. This helps identify broken links or blocked websites.

### `policy_monitor/monitor.py`

The **RSS feed fetcher**. This file downloads and parses RSS feeds from Chinese news outlets and government websites. For each feed, it:
1. Downloads the feed using an HTTP request
2. Parses the XML content using the `feedparser` library
3. Extracts up to 10 recent articles (title, link, publication date, summary)
4. Stores the results in the database

It fetches feeds concurrently (up to 8 at a time) for speed. It can be run from the command line with options to fetch all feeds or just direct feeds, filter by category, output as JSON, or show items already stored in the database.

### `policy_monitor/financial.py`

Fetches **Chinese financial market data** using the AKShare library (a Python library that provides access to Chinese financial data from sources like East Money and Jin10). It collects:

- **SHIBOR rates**: The Shanghai Interbank Offered Rate (interest rates at which Chinese banks lend to each other), across different time periods (overnight, 1 week, 1 month, etc.)
- **Government bond yields**: China Government Bond yields for 1-year, 3-year, 5-year, 10-year, and 30-year maturities
- **Stock market indices**: Shanghai Composite, Shenzhen Component, CSI 300, and ChiNext (the four major Chinese stock indices)
- **Foreign exchange rates**: USD/CNH and EUR/CNH (how much Chinese yuan one US dollar or euro buys)
- **CPI (Consumer Price Index)**: Monthly inflation data
- **PMI (Purchasing Managers' Index)**: An indicator of manufacturing sector health
- **Trade data**: Export and import year-over-year changes, trade balance

Each data series is stored as a time series (a sequence of date + value pairs) in the database, along with a "snapshot" of the most recent value.

### `policy_monitor/macro.py`

Fetches **macroeconomic data** from the Global Macro Database (GMD), an academic dataset covering 75 annual economic variables for 243 countries with data going back to 1640. This module filters for China-specific data and stores 75 variables organized into categories:

- **GDP & Growth**: Real GDP, GDP per capita, nominal GDP (in both local currency and USD)
- **Prices & Inflation**: CPI, inflation rate
- **Labor & Population**: Population, unemployment rate
- **Trade & FX**: Exports, imports, current account balance, exchange rate
- **Fiscal**: Government expenditure, revenue, tax, deficit, debt (at central, general, and consolidated government levels)
- **Monetary**: Central bank rate, interest rates, money supply (M0, M1, M2, M3)
- **Housing**: House Price Index
- **Consumption & Investment**: Household consumption, government consumption, total investment, fixed investment
- **Crisis Events**: Sovereign debt crisis, currency crisis, banking crisis indicators

It stores a "live" version (the latest data) and a "history" version (all revisions, so you can see how the data has been revised over time).

### `policy_monitor/academic.py`

Scans **top academic journals** for China-related publications using the CrossRef API (a free API for academic metadata). It monitors five journals:

- The China Quarterly (dedicated to China studies)
- American Economic Review
- Journal of Political Economy
- Quarterly Journal of Economics
- China & World Economy

For China-specific journals, all articles are included. For general economics journals, articles are filtered by China-related keywords (like "China," "Beijing," "renminbi," "belt and road," "hukou," etc.) in the title or abstract.

The module also includes a **preference learning system**: you can upvote or downvote articles, and the system learns which journals, keywords, and authors you prefer, then ranks future articles accordingly.

### `policy_monitor/bruegel.py`

Fetches data from the **Bruegel China Economic Database** (maintained by Bruegel, a European economics think tank). It downloads Excel files from their data portal containing:

- High-frequency indicators: PMI, inflation, production, consumption, retail sales, real estate, investment, exports
- Financial indicators: SHIBOR, government bond yields, exchange rates, RMB index, stock market data
- Low-frequency data: Monetary policy, shadow banking, total social financing, debt-by-sector
- Provincial profiles: GDP, population, and export value for each Chinese province

The module uses "conditional downloading" — it only re-downloads files if they have changed since the last fetch, to save bandwidth.

### `policy_monitor/dissent.py`

Fetches data from the **China Dissent Monitor** (`chinadissent.net`), a database tracking protests, strikes, and other collective action events across China. For each event, it records:

- Case ID, date, province, and location
- Whether it was online or offline
- Mode (protest, strike, banner, etc.)
- Issue (labor, housing, environment, etc.)
- Target (company, government, etc.)
- Description, number of participants, government response (repression, concession)

It also fetches a list of provinces and provides per-province summaries.

### `policy_monitor/flights.py`

Fetches **real-time flight positions** over China from the OpenSky Network API. OpenSky is a free network of ADS-B receivers (devices that pick up radio signals from aircraft) that provides live flight tracking data. The module:

- Requests all aircraft positions within China's geographic bounding box (latitude 18-54, longitude 73-135)
- Stores both a "live" table (current snapshot of all flights, replacing the previous snapshot) and a "history" table (appending every snapshot so flight paths can be reconstructed)
- Each flight record includes: ICAO24 identifier (a unique aircraft code), callsign, country of origin, position (latitude/longitude), altitude, speed, heading, and vertical rate

Anonymous access allows ~100 queries per day (1 every 15 minutes). With a free account, you get ~4,000 queries per day (1 per minute).

### `policy_monitor/ships.py`

Fetches **real-time ship positions** around China from AIS (Automatic Identification System) data providers. AIS is a tracking system used by all large ships. The module supports two data backends:

1. **AISHub** (preferred): A free REST API that returns ship positions in one HTTP request. Requires registration and sharing your own AIS receiver data.
2. **AISStream**: A WebSocket-based service (meaning a persistent connection that streams data in real time). Requires an API key.

It tries AISHub first and falls back to AISStream if AISHub is not configured. Each ship record includes: MMSI (a unique ship identifier), name, position, course, speed, heading, navigation status, and destination.

Like flights, it stores both a live snapshot and a history table.

### `policy_monitor/dashboard.py`

The **web dashboard** — the visual front end of the entire project. It is built using Flask, a Python web framework. This is a very large file that defines dozens of API endpoints (URLs that return data in JSON format) and serves the HTML pages for the dashboard.

The dashboard provides:
- A **map view** showing live flight and ship positions over China
- A **news feed** showing the latest articles from Chinese state media and other sources
- **Financial data** charts for stocks, bonds, forex, and macro indicators
- **Bruegel economic data** with category browsing and time series charts
- **Macroeconomic data** from the Global Macro Database with variable exploration
- **Academic articles** with search, filtering, voting, and preference-based ranking
- **Dissent events** browsable by province, with a geographic map
- A **source directory** showing all tracked government and media sources
- **Provincial data** comparison views

The dashboard runs at `http://127.0.0.1:5001` (only accessible on your own computer, not on the internet).

---

## Part 8: The `policy_monitor/runners/` Folder — Pipeline Orchestration

This sub-package contains the scripts that orchestrate (coordinate) the data fetching pipelines. Think of them as the "managers" that call the individual data fetchers in the right order.

### `policy_monitor/runners/__init__.py`

Empty file marking this folder as a Python package.

### `policy_monitor/runners/fetch_batch.py`

The **batch pipeline manager**. This is the primary script for collecting all non-real-time data. It sequentially runs fetchers for:

1. Financial market data (SHIBOR, bonds, stocks, forex, CPI, PMI, trade)
2. Dissent events from China Dissent Monitor
3. Bruegel economic indicators
4. Global Macro Database macroeconomic data
5. Academic journal articles
6. China Customs export data

It records each batch run in the database (start time, which sources were attempted, which succeeded, which failed, completion time). You can optionally specify which sources to fetch (for example, just financial and macro) and add a random delay before starting (to avoid always hitting data sources at the exact same time, which could look suspicious or get you rate-limited).

### `policy_monitor/runners/fetch_realtime.py`

The **real-time pipeline manager**. This runs as a continuous loop that never ends (until you stop it). On each iteration, it:

1. Fetches flight positions (every 1 minute with credentials, every 15 minutes without)
2. Fetches ship positions (every ~60 seconds)
3. Fetches RSS news feeds (every hour)

These three tasks run in separate threads (parallel execution paths within the same program), so they do not block each other. If the ship data collection takes 55 seconds, the flight fetch can still run simultaneously.

### `policy_monitor/runners/fetch_all.py` (deprecated)

An older wrapper that runs both the batch pipeline and a news fetch. The project has moved to using `fetch_batch.py` and `fetch_realtime.py` separately for better control.

### `policy_monitor/runners/fetch_macro.py` (deprecated)

An older wrapper that runs the batch pipeline with a random delay. Replaced by `fetch_batch.py --random-delay`.

### `policy_monitor/runners/fetch_news.py` (deprecated)

An older script for fetching news feeds once. Replaced by the news-fetching loop in `fetch_realtime.py`.

---

## Part 9: The `launchd/` Folder — Automatic Scheduling on macOS

This folder contains **plist files** (Property List files) that configure macOS's launchd scheduling system. These files tell macOS to automatically run the data collection scripts.

### `launchd/com.chinapolicymonitor.batch.plist`

Tells macOS to run the batch data pipeline (`run_fetch_batch.py`) **once every day at 3:00 AM**. The script itself then adds a random delay of up to 18 hours before actually fetching, so the real fetch time varies throughout the day. Output and error logs are saved to `data/logs/`.

### `launchd/com.chinapolicymonitor.realtime.plist`

Tells macOS to run the real-time data fetcher (`run_fetch_realtime.py`) **continuously**. It starts automatically when you log in (`RunAtLoad: true`), and macOS will restart it if it crashes (`KeepAlive: true`). This ensures flight and ship tracking data is always being collected in the background.

### `launchd/com.chinapolicymonitor.macro.plist` and `launchd/com.chinapolicymonitor.news.plist`

Older scheduling files for the deprecated macro and news fetchers. The batch plist has replaced the macro plist, and the realtime plist handles news.

---

## Part 10: The `tests/` Folder — Automated Testing

This folder contains **test files** — programs that verify that the code works correctly. Tests are run using a tool called `pytest`. They do not collect any data or interact with the internet; they test the logic of the code in isolation.

### `tests/__init__.py`

Empty file marking this as a Python package.

### `tests/test_db.py`

Tests the customs scraper's database functions. It creates a temporary database in memory (not on disk), then tests:
- That `init_db` creates the correct tables
- That calling `init_db` twice does not cause errors (idempotent)
- That `start_run` creates a valid run record with a unique ID
- That `finish_run` correctly updates the status, row counts, and timestamps
- That `upsert_export_rows` correctly inserts new rows, updates existing ones, and handles empty lists
- That checkpoints are correctly recorded and retrieved, and that checkpoints from different runs are isolated from each other

### `tests/test_parser.py`

Tests the customs scraper's HTML parser. It uses a "fake page" system that mimics the scraping library's API but uses `lxml` (a different HTML parsing library) instead. This allows testing without actually connecting to the customs website.

It tests:
- Number parsing (handling integers, decimals, commas, dollar signs, negative numbers, and garbage input)
- Integer parsing (extracting numbers from text like "Total: 3 records")
- HS code cleaning (removing whitespace from codes)
- Row extraction (mapping table cells to data fields, handling empty values)
- Full page parsing (extracting all rows from the HTML fixture)
- Next-page detection (finding the pagination button, detecting disabled states)
- Total row count extraction

### `tests/fixtures/sample_table.html`

A **synthetic HTML page** that simulates what the customs website's results page might look like. It contains a table with three rows of sample export data (laptops exported to the United States, Germany, and Japan) and pagination controls. This is used by `test_parser.py` to test the parsing logic. The intent is that once the real customs website is inspected, this file should be replaced with actual captured HTML.

### `tests/policy_monitor/__init__.py`

Empty file marking the test subdirectory for policy monitor tests as a package.

---

## Part 11: The `ideas/` Folder — Research Notes

### `ideas/energy-monitor.md`

A research document exploring satellite data sources for monitoring China's economy. It covers:
- SpaceKnow's manufacturing index (commercial, 3x/week)
- Planet Labs daily imagery
- VIIRS nighttime lights (free, monthly)
- AI-powered energy infrastructure detection from satellite imagery
- Foundation models and toolkits for geospatial analysis
- Recommendations for what could be integrated into CMM

This is a reference/planning document, not running code.

---

## Part 12: The `tasks/` Folder — Project Management

### `tasks/todo.md`

A task tracking file where current work items and their completion status are recorded.

---

## Part 13: Other Files and Folders

### `cmm.egg-info/`

Automatically generated metadata about the Python package. Created when you install the project in "development mode" (`pip install -e .`). Contains:
- `PKG-INFO`: Package name, version, and description
- `SOURCES.txt`: List of all source files
- `dependency_links.txt`: External dependency information
- `top_level.txt`: Names of the top-level packages (`customs_scraper`, `policy_monitor`)

You never need to edit these files — they are regenerated automatically.

### `.idea/`

Configuration files for the **PyCharm** code editor (an integrated development environment, or IDE). These store editor preferences like which files are open, code inspection settings, and version control configuration. Only relevant if you use PyCharm.

### `.pytest_cache/`

Temporary cache used by the pytest testing framework to speed up repeated test runs. Automatically generated and can be safely deleted.

### `.claude/`

Configuration for the Claude AI assistant used during development.

### `customs_scraper.log`

A log file recording the output of the customs scraper. Each time the scraper runs, it appends messages to this file describing what it did and any errors it encountered.

---

## Part 14: How Everything Fits Together

Now that you know what each file does, here is how the entire system works as a whole.

### The Data Flow

1. **Data sources** (Chinese government websites, RSS feeds, financial APIs, flight/ship tracking services, academic databases, Excel files from think tanks) provide raw information.

2. **Fetcher modules** (`monitor.py`, `financial.py`, `macro.py`, `academic.py`, `bruegel.py`, `dissent.py`, `flights.py`, `ships.py`, and the customs scraper) each know how to connect to their specific data source, download the data, and convert it into a structured format.

3. **Runner scripts** (`fetch_batch.py`, `fetch_realtime.py`) coordinate when each fetcher runs — batch sources once per day, real-time sources continuously.

4. **The database** (`data/feeds.db`) stores everything in organized tables. Each type of data has its own table(s).

5. **The dashboard** (`dashboard.py`) reads from the database and presents the data as interactive web pages with charts, tables, and maps.

### The Two Pipelines

The system is split into two pipelines that run independently:

**Batch Pipeline** (runs once daily):
```
launchd triggers at 3:00 AM
  -> run_fetch_batch.py
    -> fetch_batch.py
      -> financial.py   (stocks, bonds, forex, CPI, PMI)
      -> dissent.py     (protest/strike events)
      -> bruegel.py     (economic indicators from Excel files)
      -> macro.py       (annual macroeconomic data)
      -> academic.py    (journal articles via CrossRef)
      -> customs scraper (monthly export data)
    -> results saved to feeds.db
```

**Real-time Pipeline** (runs continuously):
```
launchd starts on login, keeps alive
  -> run_fetch_realtime.py
    -> fetch_realtime.py (infinite loop)
      -> flights.py  (every 1-15 minutes)
      -> ships.py    (every ~60 seconds)
      -> news feeds  (every hour)
    -> results saved to feeds.db
```

**Dashboard** (runs when started manually):
```
run_dashboard.py
  -> dashboard.py (Flask web server)
    -> reads from feeds.db
    -> serves web pages at http://127.0.0.1:5001
```

### The Customs Scraper (Separate Sub-system)

The customs scraper is its own self-contained system that can also be run independently:

```
python -m customs_scraper.main --bootstrap-hs-codes   (one-time setup)
python -m customs_scraper.main                         (scrape one month)
python -m customs_scraper.main --schedule              (auto-run monthly)
```

It is also integrated into the batch pipeline, so it runs as part of the daily data collection.

---

## Part 15: How to Use the System

Here is the sequence of steps to get the system running, written for someone who has never used a programming project before.

### Step 1: Install Python

Python 3.11 or newer must be installed on your computer. You can download it from `python.org` or use a package manager.

### Step 2: Install Dependencies

Open a terminal (the text-based command interface on your computer), navigate to the project folder, and run:

```
pip install -r requirements.txt
```

This downloads and installs all the external packages the project needs.

### Step 3: Create Your Configuration File

Copy the example environment file:

```
cp .env.example .env
```

Then open `.env` in a text editor and fill in any credentials you have (OpenSky account, AISStream API key, proxy address, etc.). The system works without most of these — it will simply skip data sources it cannot access.

### Step 4: Start the Dashboard

```
python run_dashboard.py
```

Then open `http://127.0.0.1:5001` in your web browser. The dashboard will be empty at first because no data has been collected yet.

### Step 5: Collect Data

Run the batch pipeline to collect data from all sources:

```
python run_fetch_batch.py
```

This may take several minutes as it contacts multiple data sources. When it finishes, refresh the dashboard to see the collected data.

### Step 6 (Optional): Start Real-time Tracking

To continuously track flights, ships, and news:

```
python run_fetch_realtime.py
```

This will keep running until you stop it (press Ctrl+C).

### Step 7 (Optional): Set Up Automatic Scheduling

To have macOS run the data collection automatically, load the launchd configurations:

```
launchctl load launchd/com.chinapolicymonitor.batch.plist
launchctl load launchd/com.chinapolicymonitor.realtime.plist
```

After this, the batch pipeline will run every day at 3 AM, and the real-time fetcher will start automatically when you log in.

---

## Part 16: Summary of Every File

For quick reference, here is a complete list of every file in the repository and what it does.

| File | Purpose |
|---|---|
| `pyproject.toml` | Project identity and build configuration |
| `requirements.txt` | List of external package dependencies |
| `.env.example` | Template for environment variables (passwords, API keys) |
| `.gitignore` | List of files Git should not track |
| `CLAUDE.md` | Instructions for the AI development assistant |
| `README.md` | Project documentation front page (placeholder) |
| `workplan-4-week-mvp.md` | 4-week development plan |
| `run_dashboard.py` | Starts the web dashboard |
| `run_fetch_batch.py` | Runs the daily batch data collection |
| `run_fetch_realtime.py` | Starts continuous flight/ship/news tracking |
| `run_fetch_all.py` | Deprecated: combined batch + news fetch |
| `run_fetch_news.py` | Deprecated: one-time news fetch |
| `run_fetch_macro.py` | Deprecated: batch fetch with random delay |
| `customs_scraper/__init__.py` | Marks folder as Python package |
| `customs_scraper/config.py` | Configuration settings (from environment variables) |
| `customs_scraper/db.py` | Database tables and operations for customs data |
| `customs_scraper/hs_codes.py` | Loads the list of product classification codes |
| `customs_scraper/countries.py` | Loads the list of destination country codes |
| `customs_scraper/bootstrap.py` | Auto-fetches HS code and country lists from the website |
| `customs_scraper/fetcher.py` | Browser-based web page downloader with retries |
| `customs_scraper/parser.py` | Extracts data from downloaded HTML pages |
| `customs_scraper/page_actions.py` | Simulates clicking and typing on the customs website |
| `customs_scraper/orchestrator.py` | Coordinates the full monthly scraping process |
| `customs_scraper/scheduler.py` | Automatic monthly scheduling |
| `customs_scraper/debug.py` | Opens a browser for manual website inspection |
| `customs_scraper/main.py` | Command-line interface for the customs scraper |
| `policy_monitor/__init__.py` | Marks folder as Python package |
| `policy_monitor/storage.py` | Core database tables for policy monitor data |
| `policy_monitor/monitor.py` | RSS news feed fetcher |
| `policy_monitor/financial.py` | Chinese financial market data fetcher |
| `policy_monitor/macro.py` | Annual macroeconomic data fetcher |
| `policy_monitor/academic.py` | Academic journal article scanner |
| `policy_monitor/bruegel.py` | Bruegel think tank economic data fetcher |
| `policy_monitor/dissent.py` | China Dissent Monitor event fetcher |
| `policy_monitor/flights.py` | Real-time flight position tracker |
| `policy_monitor/ships.py` | Real-time ship position tracker |
| `policy_monitor/dashboard.py` | Web dashboard (visual interface) |
| `policy_monitor/sources/registry.yaml` | Master directory of 60+ Chinese information sources |
| `policy_monitor/sources/loader.py` | Functions to read and filter the source registry |
| `policy_monitor/sources/validate.py` | Health check for source URLs |
| `policy_monitor/runners/__init__.py` | Marks folder as Python package |
| `policy_monitor/runners/fetch_batch.py` | Batch data collection pipeline manager |
| `policy_monitor/runners/fetch_realtime.py` | Continuous real-time data collection manager |
| `policy_monitor/runners/fetch_all.py` | Deprecated: combined fetcher |
| `policy_monitor/runners/fetch_macro.py` | Deprecated: macro fetch wrapper |
| `policy_monitor/runners/fetch_news.py` | Deprecated: news fetch wrapper |
| `tests/__init__.py` | Marks folder as Python package |
| `tests/test_db.py` | Automated tests for database operations |
| `tests/test_parser.py` | Automated tests for HTML parsing logic |
| `tests/fixtures/sample_table.html` | Sample HTML page for testing the parser |
| `tests/policy_monitor/__init__.py` | Marks folder as Python package |
| `launchd/com.chinapolicymonitor.batch.plist` | macOS schedule: daily batch fetch at 3 AM |
| `launchd/com.chinapolicymonitor.realtime.plist` | macOS schedule: continuous real-time fetcher |
| `launchd/com.chinapolicymonitor.macro.plist` | macOS schedule: deprecated macro fetch |
| `launchd/com.chinapolicymonitor.news.plist` | macOS schedule: deprecated hourly news fetch |
| `data/config.json` | API credentials for flight and ship tracking |
| `data/china_provinces.json` | Geographic data for Chinese provinces (for maps) |
| `data/china_prefectures.json` | Geographic data for Chinese prefectures (for maps) |
| `data/feeds.db` | Central SQLite database (not in Git) |
| `data/logs/` | Log files from pipeline runs (not in Git) |
| `ideas/energy-monitor.md` | Research notes on satellite economic monitoring |
| `tasks/todo.md` | Current task tracking |
| `cmm.egg-info/` | Auto-generated Python package metadata |
| `.idea/` | PyCharm editor configuration |
| `.pytest_cache/` | Test framework cache |
| `.claude/` | AI assistant configuration |
| `customs_scraper.log` | Customs scraper activity log |