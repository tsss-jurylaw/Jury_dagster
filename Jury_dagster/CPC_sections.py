import uuid
import json
import asyncio
import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dagster import asset, Output


BASE_URL = "https://lawrato.com"
INDEX_URL = "https://lawrato.com/indian-kanoon/cpc"
OUTPUT_FILE = "cpc_sections.json"

async def fetch_cpc_sections():
    async with AsyncWebCrawler(verbose=True) as crawler:
        logging.info(" Crawling CPC index page...")
        index_result = await crawler.arun(
            url=INDEX_URL,
            css_selector="div.content-left div.block-card ul li.arrow-list-fancy a"
        )

        if not index_result.success:
            logging.error(f" Failed to crawl index page: {index_result.error_message}")
            return []

        soup = BeautifulSoup(index_result.html, "html.parser")
        section_links = [
            {
                "href": urljoin(BASE_URL, a['href']),
                "text": a.get_text(strip=True)
            }
            for a in soup.select("div.content-left div.block-card ul li.arrow-list-fancy a")
        ]

        logging.info(f" Found {len(section_links)} CPC sections, orders, or appendices.")

        results = []
        for link in section_links:
            href = link['href']
            text = link['text']

            if text.startswith(('CPC Section', 'CPC Order', 'CPC Appendix', 'CPC Schedule')):
                number = text.split(' - ')[0].replace('CPC Section ', '').replace('CPC Order ', '').replace('CPC Appendix ', '').replace('CPC Schedule ', '').strip()
                title = text.split(' - ')[1] if ' - ' in text else text
            else:
                logging.warning(f"Skipping invalid text format: {text}")
                continue

            logging.info(f" Crawling: {href}")
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
                    logging.warning(f"No content found for {href}")
                    continue

                title_elem = main_div.select_one("div.col-sm-12 h1")
                section_title = title_elem.get_text(strip=True) if title_elem else title

                clean_title = (
                    section_title
                    .replace(f"Section {number} CPC - Code of Civil Procedure - ", "")
                    .replace(f"Order {number} - ", "")
                    .replace(f"Appendix {number} - ", "")
                    .replace(f"Schedule {number} - ", "")
                    .strip()
                )

                content_div = main_div.select_one("div.content-left div.content.margin-20-bottom")
                if content_div:
                    for unwanted in content_div(['script', 'style']):
                        unwanted.decompose()
                    for p in content_div.find_all('p'):
                        if 'Click here to read more' in p.get_text():
                            p.decompose()

                    description_items = content_div.select("ol > li")
                    if description_items:
                        description = "\n".join(
                            li.get_text(strip=True, separator=" ").replace("  ", " ")
                            for li in description_items
                        )
                    else:
                        description_items = content_div.select("p")
                        description = "\n".join(
                            p.get_text(strip=True, separator=" ").replace("  ", " ")
                            for p in description_items if p.get_text(strip=True)
                        ) if description_items else "No details found."
                else:
                    description = "No details found."

                question_prefix = f"{text.split(' - ')[0]}"
                section_data = {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{number}-{clean_title}")),
                    "Question": f"{question_prefix} - {clean_title}",
                    "Answer": description,
                    "url": href,
                    "category_id": "Code Of Civil Procedure_sections"
                }

                results.append(section_data)
            except Exception as e:
                logging.error(f" Parsing error at {href}: {e}")

            await asyncio.sleep(2)

        logging.info(f" Completed scraping {len(results)} CPC entries.")
        return results


@asset(name="cpc_sections_asset")
def cpc_sections_asset() -> Output[list]:
    scraped_data = asyncio.run(fetch_cpc_sections())

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)

    logging.info(f" CPC data written to {OUTPUT_FILE}")

    return Output(
        value=scraped_data,
        metadata={
            "file_path": OUTPUT_FILE,
            "total_sections": len(scraped_data)
        }
    )
