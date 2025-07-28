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
MAIN_URL = "https://lawrato.com/indian-kanoon/income-tax-act"
OUTPUT_FILE = "income_tax_act_sections.json"

async def fetch_income_tax_sections():
    """Async function to scrape Income Tax Act section details."""
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
        link_elements = soup.select("div.col-sm-8 div.content-left div.block-card ul li.arrow-list-fancy a")
        section_urls = [
            {"href": urljoin(BASE_URL, link.get('href')), "text": link.get_text(strip=True)}
            for link in link_elements
            if link.get('href') and 'section-' in link.get('href')
        ]

        logging.info(f" Found {len(section_urls)} section URLs.")
        results = []

        for link in section_urls:
            href = link['href']
            text = link['text']

            if text.startswith('Income Tax Act Section'):
                section_number = text.split('Section')[1].split('-')[0].strip()
            else:
                logging.warning(f"Skipping invalid section text: {text}")
                continue

            logging.info(f" Crawling section: {href}")
            detail_res = await crawler.arun(
                url=href,
                css_selector="div.col-sm-8 div.content-left div.block-item div.block-card div.content.margin-20-bottom"
            )

            if not detail_res.success:
                logging.error(f" Failed to fetch {href}: {detail_res.error_message}")
                continue

            try:
                soup_detail = BeautifulSoup(detail_res.html, "html.parser")
                content_div = soup_detail.select_one(
                    "div.col-sm-8 div.content-left div.block-item div.block-card div.content.margin-20-bottom"
                )

                if not content_div:
                    logging.warning(f"No content found for {href}")
                    continue

                title_elem = soup_detail.select_one("div.col-sm-12 h1")
                title = title_elem.get_text(strip=True) if title_elem else f"Section {section_number}"

                description = []
                found_description = False
                for element in content_div.find_all(['h2', 'p']):
                    if element.name == 'h2' and element.text.strip() == 'Description':
                        found_description = True
                        continue
                    if found_description and element.name == 'p':
                        para = element.get_text(strip=True, separator=" ").replace("  ", " ")
                        if para and not para.startswith('Click here to read more'):
                            description.append(para)

                description_text = "\n".join(description) if description else "No details found."

                section_data = {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{section_number}-{title}")),
                    "Question": title,
                    "Answer": description_text,
                    "url": href,
                    "category_id": "Income_Tax_Act_sections"
                }

                results.append(section_data)

            except Exception as e:
                logging.error(f"Parsing error at {href}: {e}")

            await asyncio.sleep(2)

        logging.info(f" Finished scraping {len(results)} sections.")
        return results

@asset(name="income_tax_act_sections_asset")
def income_tax_act_sections_asset() -> Output[list]:
    """Dagster asset to fetch and store Income Tax Act sections."""
    scraped_data = asyncio.run(fetch_income_tax_sections())

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



