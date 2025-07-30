import uuid
import json
import asyncio
import logging
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dagster import asset, Output


BASE_URL = "https://lawrato.com"
OUTPUT_FILE = "crpc_sections.json"


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


SECTION_URLS = [
    {"href": f"https://lawrato.com/indian-kanoon/crpc/section-{i}", "text": f"section {i} CrPC"}
    for i in range(1, 485)
]

async def fetch_crpc_sections():
    async with AsyncWebCrawler(verbose=True) as crawler:
        logging.info(f" Processing {len(SECTION_URLS)} sections.")
        results = []

        for link in SECTION_URLS:
            href = link['href']
            text = link['text']
            section_number = text.replace('section ', '').replace(' CrPC', '').strip()

            logging.info(f" Crawling section: {href}")
            res = await crawler.arun(
                url=href,
                css_selector="section#guide-detail"
            )

            if not res.success:
                logging.error(f" Failed to fetch {href}: {res.error_message}")
                continue

            try:
                soup = BeautifulSoup(res.html, "html.parser")

            
                title_elem = soup.select_one("section#guide-detail h1")
                full_title = title_elem.get_text(strip=True) if title_elem else f"Section {section_number}"

                
                if " - " in full_title:
                    _, title = full_title.split(" - ", 1)
                else:
                    title = full_title

        
                content_div = soup.select_one("section#guide-detail div.content-left div.content.margin-20-bottom")
                description = ""
                if content_div:
                    for unwanted in content_div(['script', 'style', 'div.alert.alert-info', 'p:last-child']):
                        unwanted.decompose()
                    text_elements = content_div.find_all(['p', 'li'])
                    description_parts = [elem.get_text(strip=True, separator=" ") for elem in text_elements if elem.get_text(strip=True)]
                    description = "\n".join(description_parts) if description_parts else "No details found."
                else:
                    description = "No details found."

        
                section_data = {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{section_number}-{title}")),
                    "Question": f"CrPC Section {section_number} - {title}",
                    "Answer": description,
                    "url": href,
                    "category_id": "Code of Criminal Procedure_sections"
                }

                results.append(section_data)

            except Exception as e:
                logging.error(f"Parsing error at {href}: {e}")

            await asyncio.sleep(2)

        logging.info(f" Finished scraping {len(results)} sections.")
        return results

@asset(name="crpc_sections_asset")
def crpc_sections_asset() -> Output[list]:
    scraped_data = asyncio.run(fetch_crpc_sections())

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
