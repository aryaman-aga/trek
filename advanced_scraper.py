import asyncio
import json
import logging
from typing import Optional, List, Dict
from pydantic import BaseModel, Field, field_validator
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("AsyncScraper")

# --- 1. PYDANTIC DATA VALIDATION ---
class BikeRow(BaseModel):
    brand: str
    model_name: str = Field(..., min_length=2)
    segment: str = ""
    listed_price: Optional[float] = None
    discounted_price: Optional[float] = None
    currency: str = "INR"
    url: Optional[str] = None
    
    @field_validator("listed_price", "discounted_price", mode="before")
    def validate_price(cls, v):
        if v is None or v == "":
            return None
        try:
            val = float(str(v).replace(",", "").strip())
            return val if val > 0 else None
        except ValueError:
            return None

# --- 2. BASE OOP SCRAPER ARCHITECTURE ---
class BaseScraper:
    def __init__(self, brand_cfg: Dict, proxy_url: str = None):
        self.cfg = brand_cfg
        self.brand_name = brand_cfg["name"]
        self.base_url = brand_cfg["url"]
        self.proxy = {"all://": proxy_url} if proxy_url else None
        
    async def scrape(self) -> List[BikeRow]:
        raise NotImplementedError("Subclasses must implement scrape()")

    def _normalize(self, text: str) -> str:
        return text.strip() if isinstance(text, str) else ""

# --- 3. SPECIFIC IMPLEMENTATIONS ---
class ShopifyAsyncScraper(BaseScraper):
    async def scrape(self) -> List[BikeRow]:
        products_url = f"{self.base_url.rstrip('/')}/products.json?limit=250"
        results = []
        
        async with httpx.AsyncClient(proxies=self.proxy, timeout=30.0) as client:
            logger.info(f"[{self.brand_name}] Fetching Shopify API -> {products_url}")
            try:
                response = await client.get(products_url)
                if response.status_code == 200:
                    data = response.json()
                    for prod in data.get("products", []):
                        title = prod.get("title", "")
                        
                        url = f"{self.base_url}/products/{prod.get('handle')}"
                        variants = prod.get("variants", [])
                        if not variants: continue
                        
                        price = variants[0].get("price")
                        compare_at_price = variants[0].get("compare_at_price")
                        
                        try:
                            # Validation via Pydantic
                            row = BikeRow(
                                brand=self.brand_name,
                                model_name=title,
                                segment=prod.get("product_type", ""),
                                listed_price=compare_at_price or price,
                                discounted_price=price,
                                url=url
                            )
                            results.append(row)
                        except Exception as e:
                            logger.warning(f"[{self.brand_name}] Validation failed for {title}: {e}")
            except Exception as e:
                logger.error(f"[{self.brand_name}] HTTP Request Failed: {e}")
                
        return results

class PlaywrightJsonLdScraper(BaseScraper):
    """Uses Headless Playwright for JS-rendered JSON-LD bikes. Avoids bot traps."""
    async def scrape(self) -> List[BikeRow]:
        results = []
        async with async_playwright() as p:
            # Connect through a proxy if supplied
            browser_kwargs = {"headless": True}
            if self.proxy:
                browser_kwargs["proxy"] = {"server": self.proxy.get("all://")}
                
            browser = await p.chromium.launch(**browser_kwargs)
            page = await browser.new_page()
            logger.info(f"[{self.brand_name}] Rendering via Playwright Headless -> {self.base_url}")
            try:
                await page.goto(self.base_url, wait_until="domcontentloaded", timeout=45000)
                
                # Extract all JSON-LD Scripts
                json_ld_scripts = await page.locator("script[type='application/ld+json']").all_inner_texts()
                for script in json_ld_scripts:
                    try:
                        data = json.loads(script)
                        if isinstance(data, dict) and data.get("@type") == "Product":
                            title = data.get("name", "")
                            offers = data.get("offers", {})
                            price = offers.get("price") if isinstance(offers, dict) else None
                            
                            row = BikeRow(
                                brand=self.brand_name,
                                model_name=title,
                                listed_price=price,
                                discounted_price=price,
                                url=self.base_url
                            )
                            results.append(row)
                    except json.JSONDecodeError:
                        pass
            except Exception as e:
                 logger.error(f"[{self.brand_name}] Playwright error: {e}")
            finally:
                await browser.close()
                
        return results

# --- 4. ASYNC ORCHESTRATOR ENGINE ---
class ScraperEngine:
    def __init__(self, proxy_pool: List[str] = None):
        self.proxy_pool = proxy_pool or []
        self.proxy_index = 0
        
    def _next_proxy(self):
        if not self.proxy_pool: return None
        p = self.proxy_pool[self.proxy_index]
        self.proxy_index = (self.proxy_index + 1) % len(self.proxy_pool)
        return p

    def build_scraper(self, cfg: Dict) -> BaseScraper:
        method = cfg.get("method", "html_jsonld")
        proxy = self._next_proxy()
        
        if method == "shopify":
            return ShopifyAsyncScraper(cfg, proxy_url=proxy)
        elif method == "playwright_jsonld":
            return PlaywrightJsonLdScraper(cfg, proxy_url=proxy)
        else:
            return ShopifyAsyncScraper(cfg, proxy_url=proxy)

    async def run_all(self, brands_cfg: List[Dict]):
        tasks = []
        for cfg in brands_cfg:
            scraper = self.build_scraper(cfg)
            tasks.append(scraper.scrape())
            
        logger.info(f"Gathering async tasks for {len(brands_cfg)} brands ...")
        # Fire off all network requests non-blocking in parallel
        results_matrix = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_rows: List[BikeRow] = []
        for res in results_matrix:
            if isinstance(res, Exception):
                logger.error(f"Task Failed with Exception: {res}")
            elif res:
                all_rows.extend(res)
                
        return all_rows

# --- RUN EXECUTION ---
async def main():
    SAMPLE_BRANDS = [
        {"name": "Polygon Bikes", "url": "https://www.polygonbikes.com", "method": "shopify"},
        {"name": "Felt Bicycles", "url": "https://feltbicycles.com", "method": "shopify"}
    ]
    
    engine = ScraperEngine() 
    print("🚀 Starting Advanced Async Scraper Engine...")
    start_t = datetime.now()
    outputs: List[BikeRow] = await engine.run_all(SAMPLE_BRANDS)
    duration = datetime.now() - start_t
    print(f"✅ Scraping completed in {duration.total_seconds():.2f}s")
    
    export_data = [row.model_dump() for row in outputs]
    df = pd.DataFrame(export_data)
    
    out_file = "async_scraped_data_validated.xlsx"
    df.to_excel(out_file, index=False)
    print(f"💾 Validated DataFrame saved to {out_file} - Shape: {df.shape}")

if __name__ == "__main__":
    asyncio.run(main())