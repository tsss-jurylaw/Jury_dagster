import os
import uuid
import asyncio
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dagster import asset, Output

BASE_URL = "https://lawrato.com"
MAIN_URL = "https://lawrato.com/free-legal-advice"
OUTPUT_FILE = "legal_forms_questions_full.json"


async def fetch_legal_questions():
    async with AsyncWebCrawler(verbose=True) as crawler:
        print(" Crawling main category page...")
        index_result = await crawler.arun(
            url=MAIN_URL,
            css_selector="div.col-lg-12.col-xs-12.colMb.venue-amenities ul li.arrow-list-fancy a"
        )

        if not index_result.success:
            print(f" Failed to crawl categories: {index_result.error_message}")
            return []

        soup = BeautifulSoup(index_result.html, "html.parser")
        category_links = soup.select("div.col-lg-12.col-xs-12.colMb.venue-amenities ul li.arrow-list-fancy a")
        categories = [{
            "url": urljoin(BASE_URL, a["href"]),
            "name": a.get_text(strip=True)
        } for a in category_links if a.get("href")]

        print(f" Found {len(categories)} categories.")

        all_results = []

        for category in categories:
            cat_name = category["name"]
            cat_url = category["url"]
            page = 1

            while True:
                page_url = f"{cat_url}?page={page}"
                print(f" Crawling category '{cat_name}' - Page {page}")
                page_result = await crawler.arun(url=page_url)

                if not page_result.success:
                    print(f" Failed to fetch {page_url}: {page_result.error_message}")
                    break

                soup = BeautifulSoup(page_result.html, "html.parser")
                questions = soup.select("div.question-box.border-box.hidden-xs")

                if not questions:
                    print(f" No questions found on page {page} for '{cat_name}'")
                    break

                for q in questions:
                    title_tag = q.select_one("div.question-title h2")
                    link_tag = q.select_one("div.question-title a")
                    if not title_tag or not link_tag:
                        continue

                    question_title = title_tag.get_text(strip=True)
                    question_url = urljoin(BASE_URL, link_tag.get("href"))

                    print(f" Fetching detail page: {question_url}")
                    detail_result = await crawler.arun(url=question_url)

                    if not detail_result.success:
                        print(f" Failed to fetch detail page {question_url}")
                        continue

                    try:
                        detail_soup = BeautifulSoup(detail_result.html, "html.parser")
                        answer_div = detail_soup.select_one("div.question-body div[itemprop='text']")
                        full_answer = answer_div.get_text("\n", strip=True) if answer_div else "No answer found."

                        qa_data = {
                            "id": str(uuid.uuid5(uuid.NAMESPACE_URL, question_url)),
                            "Question": question_title,
                            "Answer": full_answer,
                            "url": question_url,
                            "category_id": "Legal queries"
                        }

                        all_results.append(qa_data)
                        print(f" Scraped Q: {qa_data['Question']}")

                    except Exception as e:
                        print(f" Error parsing {question_url}: {e}")

                next_button = soup.find("a", string="»")
                if not next_button:
                    print(f" Done with category '{cat_name}'")
                    break

                page += 1

        print(f" Finished scraping. Total Q&As collected: {len(all_results)}")
        return all_results


@asset(name="legal_questions_asset")
def legal_questions_asset() -> Output[list]:
    scraped_data = asyncio.run(fetch_legal_questions())

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)
    print(f" Saved data to {OUTPUT_FILE}")

    return Output(
        value=scraped_data,
        metadata={
            "file_path": OUTPUT_FILE,
            "total_records": len(scraped_data)
        }
    )
