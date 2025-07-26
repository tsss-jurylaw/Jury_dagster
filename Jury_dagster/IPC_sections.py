import os
import uuid
import asyncio
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dagster import asset, Output

BASE_URL = "https://devgan.in"
INDEX_URL = "https://devgan.in/all_sections_ipc.php"
OUTPUT_FILE = "ipc_sections.json"

async def fetch_ipc_sections():
    async with AsyncWebCrawler(verbose=True) as crawler:
        print(" Crawling index page...")
        index_result = await crawler.arun(
            url=INDEX_URL,
            css_selector="div#content a[href^='/ipc/section/']"
        )

        if not index_result.success:
            print(f" Failed to crawl index page: {index_result.error_message}")
            return []

        soup = BeautifulSoup(index_result.html, "html.parser")
        section_links = [urljoin(BASE_URL, a['href']) for a in soup.select("div#content a[href^='/ipc/section/']")]

        results = []
        for url in section_links:
            print(f" Crawling section: {url}")
            res = await crawler.arun(url=url)

            if not res.success:
                print(f" Failed to fetch {url}: {res.error_message}")
                continue

            try:
                soup2 = BeautifulSoup(res.html, "html.parser")
                number = soup2.select_one("tr.mys-head td.nowrap h2 strong").text.strip()
                title = soup2.select_one("tr.mys-head td:nth-of-type(2) h2").text.strip()
                desc = "\n".join(t.get_text(strip=True) for t in soup2.select("tr.mys-desc td *"))

                section_data = {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{number}-{title}")),
                    "Question": f"S. {number} {title}",
                    "Answer": desc,
                    "url": url, 
                    "category_id": "IPC_sections"
                }

                results.append(section_data)
                print(f" Scraped: {section_data['Question']}")

            except Exception as e:
                print(f" Parsing error at {url}: {e}")

        print(f" Finished scraping {len(results)} sections.")
        return results

@asset(name="ipc_sections_asset")
def ipc_sections_asset() -> Output[list]:
    scraped_data = asyncio.run(fetch_ipc_sections())

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)
    print(f" Saved data to {OUTPUT_FILE}")

    return Output(
        value=scraped_data,
        metadata={
            "file_path": OUTPUT_FILE,
            "total_sections": len(scraped_data)
        }
    )


