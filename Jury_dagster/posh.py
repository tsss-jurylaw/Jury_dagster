import logging
import uuid
import json
import re
import asyncio
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dagster import asset, Output

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_URL = "https://lawrato.com"
MAIN_URL = "https://lawrato.com/indian-kanoon/posh-act"
OUTPUT_FILE = "posh_act_sections.json"

@asset(name="posh_act_sections_asset")
async def posh_act_sections_asset() -> Output[list]:
    """Dagster asset to scrape POSH Act sections."""

    async def get_section_urls(crawler):
        """Get section URLs from main POSH page."""
        logging.info(f" Crawling main page: {MAIN_URL}")
        res = await crawler.arun(
            url=MAIN_URL,
            css_selector="div.block-card ul li.arrow-list-fancy a"
        )

        if not res.success:
            logging.error(f" Failed to fetch {MAIN_URL}: {res.error_message}")
            return []

        try:
            soup = BeautifulSoup(res.html, "html.parser")
            link_elements = soup.select("div.block-card ul li.arrow-list-fancy a")
            section_urls = []

            for link in link_elements:
                href = link.get('href')
                if href and 'section-' in href:
                    text = re.sub(r'\s+', ' ', link.get_text(strip=True)) 
                    section_urls.append({
                        "href": urljoin(BASE_URL, href),
                        "question": text
                    })
            logging.info(f" Found {len(section_urls)} section URLs.")
            return section_urls
        except Exception as e:
            logging.error(f"Error parsing main page: {e}")
            return []

    results = []

    async with AsyncWebCrawler(verbose=True) as crawler:
        section_urls = await get_section_urls(crawler)
        if not section_urls:
            return Output([], metadata={"error": "No sections found"})

        logging.info(f" Processing {len(section_urls)} sections")

        for link in section_urls:
            href = link['href']
            question = link['question']

            match = re.search(r'Section\s+(\d+)', question)
            if match:
                section_number = match.group(1)
            else:
                logging.warning(f" Skipping unrecognized section format: {question}")
                continue

            logging.info(f"Crawling section: {href}")
            res = await crawler.arun(
                url=href,
                css_selector="section#guide-detail div.container div.row div.col-sm-8"
            )

            if not res.success:
                logging.warning(f" Failed to fetch {href}: {res.error_message}")
                continue

            try:
                soup = BeautifulSoup(res.html, "html.parser")
                main_div = soup.select_one("section#guide-detail div.container div.row div.col-sm-8")
                if not main_div:
                    logging.warning(f"No content found for: {href}")
                    continue

                content_div = main_div.select_one("div.content-left div.block-item div.block-card div.content.margin-20-bottom")
                if content_div:
                    for unwanted in content_div(['script', 'style', 'div.alert.alert-info', 'p:last-child']):
                        unwanted.decompose()

                    text_elements = content_div.find_all(['p', 'li', 'h2', 'h3'])
                    description_parts = [
                        elem.get_text(strip=True, separator=" ").replace("  ", " ")
                        for elem in text_elements
                        if elem.get_text(strip=True) and elem.get_text(strip=True).lower() != "description"
                    ]
                    description = "\n".join(description_parts) if description_parts else "No details found."
                else:
                    description = "No details found."

                section_data = {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{section_number}-{question}")),
                    "Question": question,
                    "Answer": description,
                    "url": href,
                    "category_id": "POSH_sections"
                }

                results.append(section_data)
                await asyncio.sleep(2)

            except Exception as e:
                logging.error(f"Parsing error for {href}: {e}")
                continue

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logging.info(f" Data saved to {OUTPUT_FILE}")

    return Output(
        value=results,
        metadata={
            "file_path": OUTPUT_FILE,
            "total_sections": len(results)
        }
    )
