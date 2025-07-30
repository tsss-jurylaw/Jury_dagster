import os
import json
import uuid
import re
import asyncio
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dagster import asset, Output

BASE_URL = "https://lawrato.com"
MAIN_URL = "https://lawrato.com/indian-kanoon/hindu-marriage-act"
OUTPUT_FILE = "hindu_marriage_act_sections.json"

async def fetch_hma_sections():
    """Async function to fetch and scrape all Hindu Marriage Act sections."""
    async with AsyncWebCrawler(verbose=True) as crawler:
        print(f" Crawling main page: {MAIN_URL}")
        index_result = await crawler.arun(
            url=MAIN_URL,
            css_selector="div.col-sm-8 div.content-left div.block-card ul li.arrow-list-fancy a"
        )

        if not index_result.success:
            print(f" Failed to crawl main page: {index_result.error_message}")
            return []

        soup = BeautifulSoup(index_result.html, "html.parser")
        section_links = []

        for link in soup.select("div.col-sm-8 div.content-left div.block-card ul li.arrow-list-fancy a"):
            href = link.get("href")
            if href and "section-" in href:
                full_text = re.sub(r'\s+', ' ', link.get_text(strip=True))
                section_links.append({
                    "href": urljoin(BASE_URL, href),
                    "question": full_text
                })

        print(f" Found {len(section_links)} section links")

        results = []
        for section in section_links:
            href = section["href"]
            question = section["question"]

            match = re.search(r'Section\s+(\d+)', question)
            if not match:
                print(f" Skipping invalid format: {question}")
                continue
            section_number = match.group(1)

            print(f" Crawling section: {href}")
            res = await crawler.arun(
                url=href,
                css_selector="section#guide-detail div.container div.row div.col-sm-8"
            )

            if not res.success:
                print(f" Failed to fetch {href}: {res.error_message}")
                continue

            try:
                soup2 = BeautifulSoup(res.html, "html.parser")
                main_div = soup2.select_one("section#guide-detail div.container div.row div.col-sm-8")
                content_div = main_div.select_one("div.content-left div.block-item div.block-card div.content.margin-20-bottom")

                description = "No details found."
                if content_div:
                    for unwanted in content_div(['script', 'style', 'div.alert.alert-info', 'p:last-child']):
                        unwanted.decompose()
                    description_parts = []
                    for elem in content_div.find_all(['p', 'li', 'h2', 'h3']):
                        text = elem.get_text(strip=True, separator=" ").replace("  ", " ")
                        if text.lower() != "description":
                            description_parts.append(text)
                    description = "\n".join(description_parts) if description_parts else "No details found."

                results.append({
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{section_number}-{question}")),
                    "Question": question,
                    "Answer": description,
                    "url": href,
                    "category_id": "HMA_sections"
                })

            except Exception as e:
                print(f" Error parsing section {href}: {e}")
            await asyncio.sleep(2)

        print(f" Scraped {len(results)} HMA sections.")
        return results 

@asset(name="hma_sections_asset")
def hma_sections_asset() -> Output[list]:
    """Dagster asset to scrape and save Hindu Marriage Act sections."""
    scraped_data = asyncio.run(fetch_hma_sections())

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