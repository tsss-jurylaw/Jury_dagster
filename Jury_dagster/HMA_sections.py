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
MAIN_URL = "https://lawrato.com/indian-kanoon/hindu-marriage-act"
OUTPUT_FILE = "hindu_marriage_act_sections.json"

async def fetch_hma_sections():
    """Scrapes all Hindu Marriage Act sections with title and content."""
    async with AsyncWebCrawler(verbose=True) as crawler:
        logging.info(f" Crawling main page: {MAIN_URL}")
        res = await crawler.arun(
            url=MAIN_URL,
            css_selector="div.col-sm-8 div.content-left div.block-card ul li.arrow-list-fancy a"
        )

        if not res.success:
            logging.error(f" Failed to fetch {MAIN_URL}: {res.error_message}")
            return []

        soup = BeautifulSoup(res.html, "html.parser")
        links = soup.select("div.col-sm-8 div.content-left div.block-card ul li.arrow-list-fancy a")
        section_links = [
            {"href": urljoin(BASE_URL, a['href']), "text": a.get_text(strip=True)}
            for a in links if 'href' in a.attrs and 'section-' in a['href']
        ]

        results = []
        for link in section_links:
            href = link['href']
            text = link['text']
            section_number = text.split('Section')[1].split('-')[0].strip() if 'Section' in text else "Unknown"

            logging.info(f" Crawling section: {href}")
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

                title_elem = main_div.select_one("div.col-sm-12 h1")
                title = title_elem.get_text(strip=True) if title_elem else f"Section {section_number}"

                content_div = main_div.select_one("div.content-left div.block-item div.block-card div.content.margin-20-bottom")
                if content_div:
                    for unwanted in content_div(['script', 'style', 'div.alert.alert-info', 'p:last-child']):
                        unwanted.decompose()
                    parts = [e.get_text(strip=True, separator=" ").replace("  ", " ")
                             for e in content_div.find_all(['p', 'li', 'h2', 'h3'])
                             if e.get_text(strip=True) and e.get_text(strip=True) != "Description"]
                    description = "\n".join(parts) if parts else "No details found."
                else:
                    description = "No details found."

                section_data = {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{section_number}-{title}")),
                    "Question": f"Hindu Marriage Act Section {section_number} - {title}",
                    "Answer": description,
                    "url": href,
                    "category_id": "HMA_sections"
                }

                results.append(section_data)
                await asyncio.sleep(2)

            except Exception as e:
                logging.error(f"Error parsing {href}: {e}")
                continue

        return results


@asset(name="hma_sections_asset")
def hma_sections_asset() -> Output[list]:
    """Dagster asset to scrape Hindu Marriage Act sections."""
    scraped_data = asyncio.run(fetch_hma_sections())

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
