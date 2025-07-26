
import os
import uuid
import asyncio
import json
import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dagster import asset, Output


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


BASE_URL = "https://lawrato.com"
INDEX_URL = "https://lawrato.com/bharatiya-nyaya-sanhita"
OUTPUT_FILE = "bns_sections.json"

async def fetch_bns_sections():
    async with AsyncWebCrawler(verbose=True) as crawler:
        logging.info(" Crawling index page...")
        index_result = await crawler.arun(
            url=INDEX_URL,
            css_selector="div.block-card ul.bns-list li.arrow-list-fancy a"
        )

        if not index_result.success:
            logging.error(f"Failed to crawl index page: {index_result.error_message}")
            return []

        soup = BeautifulSoup(index_result.html, "html.parser")
        section_links = [
            {
                "href": urljoin(BASE_URL, a['href']),
                "text": a.get_text(strip=True)
            }
            for a in soup.select("div.block-card ul.bns-list li.arrow-list-fancy a")
        ]

        logging.info(f" Processing {len(section_links)} sections.")

        results = []
        for link in section_links:
            href = link['href']
            text = link['text']

            if text.startswith('BNS Section'):
                section_number = text.split(' - ')[0].replace('BNS Section ', '').strip()
                section_title = text.split(' - ')[1].strip() if ' - ' in text else ''
            else:
                logging.warning(f"Skipping invalid section text: {text}")
                continue

            logging.info(f" Crawling section: {href}")
            res = await crawler.arun(
                url=href,
                css_selector="section#guide-detail div.container div.row div.col-sm-8"
            )

            if not res.success:
                logging.error(f" Failed to fetch {href}: {res.error_message}")
                continue

            try:
                soup2 = BeautifulSoup(res.html, "html.parser")
                main_div = soup2.select_one("section#guide-detail div.container div.row div.col-sm-8")
                if not main_div:
                    logging.warning(f"No main content found for {href}")
                    continue

                content_div = main_div.select_one("div.content-left")
                if content_div:
                    for unwanted in content_div(['script', 'style']):
                        unwanted.decompose()
                    description = "\n".join(t.get_text(strip=True) for t in content_div.find_all(recursive=False))
                else:
                    description = "No details found."

                section_data = {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{section_number}-{section_title}")),
                    "Question": f"BNS Section {section_number} - {section_title}",
                    "Answer": description,
                    "url": href,
                    "category_id": "BNS_sections"
                }

                results.append(section_data)

            except Exception as e:
                logging.error(f"Parsing error at {href}: {e}")

            await asyncio.sleep(1)

        logging.info(f" Finished scraping {len(results)} sections.")
        return results

@asset(name="bns_sections_asset")
def bns_sections_asset() -> Output[list]:
    scraped_data = asyncio.run(fetch_bns_sections())

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)
    logging.info(f" Data saved to {OUTPUT_FILE}")

    return Output(
        value=scraped_data,
        metadata={
            "file_path": OUTPUT_FILE,
            "total_sections": len(scraped_data)
        }
    )
