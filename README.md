# Cycle Brand Price Scraper

A robust suite of multi-threaded web scrapers designed to aggregate bicycle price data across Indian and Global cycle brands. Features an interactive Streamlit UI dashboard designed for non-technical users to effortlessly execute scraping operations, observe live logs, and download timestamped Excel reports.

## Features
- **User-Friendly Dashboard**: Browser-based Streamlit UI with live progress indicators.
- **Support for Major Brands**: Includes predefined crawling logic (JSON-LD, GraphQL OCC, native Shopify API, bespoke HTML parsing) for brands like Trek, BMC, Giant, Firefox, Polygon, and more. 
- **Automated Price Conversions**: Optional automatic toggling of foreign cycle prices into Indian Rupees (INR).
- **Concurrency-Safe Excel Exports**: Safely produces unique `xlsx` files labeled with the run date and time.

## Requirements
- Python 3.9+
- `pip install -r requirements.txt`

### Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone <your-repository-url>
   cd <your-repository-directory>
   ```

2. **Initialize a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   playwright install
   ```

## Usage

**Run the Streamlit Dashboard (Recommended):**
```bash
streamlit run app.py
```
This will open `http://localhost:8501` in your browser. From here, you can select which scraper pipeline to use ("Global Brands" or "All Indian Cycle Brands"), pick specific brands via the multi-select input, toggle currency conversion, and execute.

### Manual CLI Execution
If preferred, you can run the individual scrapers in the terminal:
```bash
# Global Brands
python scrape_competition_matrix.py trek bmc --inr --out my_custom_report.xlsx

# Indian Brands
python scrape_all_brands.py --out indian_bikes.xlsx
```

## Maintenance Notes
- **Anti-Bot Bypass**: Evasion logic includes randomized User-Agent rotations, `curl_cffi` impersonation where standard requests fail, and randomized sleep intervals.
- The `scrape_competition_matrix.py` and `scrape_all_brands.py` scripts act as the asynchronous backend workers. The `app.py` wrapper orchestrates them using non-blocking buffered subprocesses.
