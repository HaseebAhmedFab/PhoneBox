import asyncio
import httpx
from bs4 import BeautifulSoup
import re
import random
from typing import Tuple, List
import time  # For total time measurement
import os
import csv   # <-- NEW
from datetime import datetime  # <-- NEW
from dotenv import load_dotenv

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()
ZYTE_API_KEY = os.getenv("ZYTE_API_KEY")
ZYTE_ENDPOINT = os.getenv("ZYTE_ENDPOINT")

# ----------------------------
# Helper functions
# ----------------------------

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = re.sub(r'^-|-$', '', text)
    return text

def build_url(device: dict) -> str:
    condition_map = {
        "new": "new",
        "broken": "broken",
        "good": "working",
        "poor": "working-poor",
    }
    model_slug = slugify(device['mobile_model'])
    condition_slug = condition_map.get(device['condition'].lower(), device['condition'].lower())
    return (
        f"https://www.compareandrecycle.co.uk/mobile-phones/{model_slug}"
        f"?capacity={device['capacity']}&network={device['network']}&condition={condition_slug}"
    )

def extract_data(response_text: str) -> Tuple[str, str]:
    if not response_text or "<html" not in response_text.lower():
        return "Price not found", "Company not found"

    soup = BeautifulSoup(response_text, 'html.parser')
    container = soup.select_one('#top-recommended-container')

    price = ""
    if container:
        for div in container.select('span.price div'):
            m = re.search(r'£\s*([\d\.,]+)', div.get_text(strip=True))
            if m:
                price = m.group(1).replace(',', '')
                break

    company = ""
    if container:
        for img in container.select('img.top-recommended-image'):
            alt = img.get('alt', '').strip()
            if alt and 'facebook' not in alt.lower():
                company = alt
                break

    if not price or not company:
        match = re.search(r'<div id=["\']top-recommended-container["\']>(.*?)</div>\s*</div>',
                          response_text, re.DOTALL | re.IGNORECASE)
        if match:
            block = match.group(1)
            if not price:
                price_match = re.search(r'£\s*([\d\.,]+)', block)
                if price_match:
                    price = price_match.group(1).replace(',', '')
            if not company:
                company_match = re.search(r'<img[^>]+alt=["\']([^"\']+)["\']', block)
                if company_match and 'facebook' not in company_match.group(1).lower():
                    company = company_match.group(1)

    return price if price else "Price not found", company if company else "Company not found"

def format_price(price_raw: str) -> str:
    if not price_raw or price_raw == "Price not found":
        return "Price not found"
    return f"£{price_raw}" if not price_raw.startswith("£") else price_raw

# ----------------------------
# Async fetcher
# ----------------------------

async def fetch_device_data(device: dict, semaphore: asyncio.Semaphore) -> Tuple[str, str, str, str, str, str, str]:
    async with semaphore:
        url = build_url(device)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

        actions_full = [
            {"action": "waitForTimeout", "timeout": 2},
            {"action": "waitForSelector", "selector": {"type": "css", "value": "#top-recommended-container"}, "timeout": 10},
            {"action": "scrollBottom"},
            {"action": "waitForTimeout", "timeout": 1},
        ]
        actions_simple = [
            {"action": "waitForTimeout", "timeout": 2},
            {"action": "scrollBottom"},
        ]

        delay = 2.0

        async with httpx.AsyncClient(timeout=90) as client:
            for attempt in range(1, 4):
                chosen_actions = actions_full if attempt == 1 else actions_simple
                payload = {
                    "url": url,
                    "browserHtml": True,
                    "geolocation": "GB",
                    "javascript": True,
                    "actions": chosen_actions,
                }

                try:
                    resp = await client.post(ZYTE_ENDPOINT, auth=(ZYTE_API_KEY, ""), json=payload, headers=headers)
                    if resp.status_code == 200:
                        data = resp.json()
                        html = data.get("browserHtml", "")
                        if html:
                            price_raw, company = extract_data(html)
                            price = format_price(price_raw)
                            return (
                                device['mobile_model'],
                                device['capacity'].upper(),
                                device['condition'].capitalize(),
                                device['network'].capitalize(),
                                price,
                                company,
                                url
                            )
                    await asyncio.sleep(delay + random.random())
                    delay *= 2
                except httpx.RequestError:
                    await asyncio.sleep(delay + random.random())
                    delay *= 2

        return (
            device['mobile_model'],
            device['capacity'].upper(),
            device['condition'].capitalize(),
            device['network'].capitalize(),
            "Price not found",
            "Company not found",
            url
        )

# ----------------------------
# Runner
# ----------------------------

async def run_all_devices(devices: List[dict], concurrency_limit: int = 5):
    semaphore = asyncio.Semaphore(concurrency_limit)
    tasks = [fetch_device_data(d, semaphore) for d in devices]
    results = await asyncio.gather(*tasks)
    return results


def upsert_to_csv(
    rows: List[Tuple[str, str, str, str, str, str, str]],
    filepath: str,
    key_idx: Tuple[int, int, int, int] = (0, 1, 2, 3),  # Product, Capacity, Condition, Network
) -> None:

    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    header = ["Product", "Capacity", "Condition", "Network Lock", "Max Price", "Company", "URL"]

    existing = {}
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            existing_header = next(reader, None)
            for r in reader:
                if not r:
                    continue
                k = tuple(r[i] for i in key_idx)
                existing[k] = r

    # upsert new rows
    for r in rows:
        k = tuple(r[i] for i in key_idx)
        existing[k] = list(r)

    # write back
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for k in sorted(existing.keys()):
            writer.writerow(existing[k])

# ----------------------------
# Main function
# ----------------------------

async def main():
    devices = [
        {"mobile_model": "Samsung Galaxy S23", "capacity": "256gb", "network": "three", "condition": "good"},
        {"mobile_model": "Apple iPhone 11 Pro", "capacity": "256gb", "network": "three", "condition": "new"},
        {"mobile_model": "Apple iPhone 12 Pro Max", "capacity": "256gb", "network": "unlocked", "condition": "good"},
        {"mobile_model": "Samsung Galaxy S23 Ultra", "capacity": "256gb", "network": "unlocked", "condition": "good"},
        {"mobile_model": "Apple iPhone 14 plus", "capacity": "256gb", "network": "unlocked", "condition": "good"},
        {"mobile_model": "Samsung Galaxy S22 Ultra 5G", "capacity": "1tb", "network": "virgin", "condition": "good"},
        {"mobile_model": "Apple iPhone 15 Pro Max", "capacity": "1tb", "network": "virgin", "condition": "poor"},
        {"mobile_model": "Samsung Galaxy S22 5G", "capacity": "256gb", "network": "virgin", "condition": "poor"},
    ]

    print("Product,Capacity,Condition,Network Lock,Max Price,Company,URL")
    
    start_time = time.time()
    results = await run_all_devices(devices, concurrency_limit=5)
    end_time = time.time()

    for row in results:
        print(",".join(row))

    # ---- NEW: write CSV ----
    out_path = "outputs/scrape_results.csv"
    upsert_to_csv(results, out_path)
    print(f"\n📄 CSV updated -> {out_path}")     
    
    print(f"\n⏱ Total scraping time: {end_time - start_time:.2f} seconds")

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    asyncio.run(main())
