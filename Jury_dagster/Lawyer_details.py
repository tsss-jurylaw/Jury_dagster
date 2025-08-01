import os
import uuid
import asyncio
import json
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dagster import asset, Output

BASE_URL = "https://lawrato.com"
OUTPUT_FILE = "lawyers_category.json"

CATEGORY_URLS = {
    "Criminal_Lawyers": "https://lawrato.com/criminal-lawyers",
    "Property_Lawyers": "https://lawrato.com/property-lawyers",
    "Cyber_Crime_Lawyers": "https://lawrato.com/cyber-crime-lawyers",
    "Divorce_Lawyers": "https://lawrato.com/divorce-lawyers"
}

MAX_LAWYERS_PER_CATEGORY = 50
MAX_RETRIES = 3

def clean_text(text):
    """Clean text by removing unwanted characters, extra commas, and category names."""
    if not text:
        return None
    text = re.sub(r'[\t\n\xa0]+', ' ', text)
    for category in CATEGORY_URLS.keys():
        text = text.replace(category, '')
    text = re.sub(r'\s*,\s*', ', ', text.strip())
    text = text.strip(', ')
    text = re.sub(r',+', ', ', text)
    return text if text else None

async def fetch_page(crawler, url, css_selector):
    """Fetch a page with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = await crawler.arun(url=url, css_selector=css_selector)
            if result.success:
                return result.html
            else:
                print(f"[WARN] Crawl failed for {url}: {result.error_message}")
        except Exception as e:
            print(f"[WARN] Exception on attempt {attempt} for {url}: {e}")
        await asyncio.sleep(2)
    return None

async def crawl_lawyer_profile(crawler, profile_url):
    """Crawl a lawyer's profile page to extract specialization, courts, and languages."""
    html = await fetch_page(crawler, profile_url, "section#lawyer-detail")
    if not html:
        print(f"[ERROR] Failed to crawl profile {profile_url}")
        return None, None, None

    try:
        soup = BeautifulSoup(html, "html.parser")
        
        specialization = None
        try:
            spec_desktop = soup.select_one("div.hidden-xs div.label-detail:-soup-contains('Specialization') + div.col-xs-11")
            if spec_desktop:
                specialization = clean_text(spec_desktop.get_text(strip=True, separator=', '))
            else:
                spec_mobile = soup.select_one("div.visible-xs div.label-detail:-soup-contains('Specialization') + div.panel1")
                if spec_mobile:
                    specialization = clean_text(spec_mobile.get_text(strip=True, separator=', '))
        except Exception as e:
            print(f"[WARN] Failed to extract specialization for {profile_url}: {e}")

        courts = None
        try:
            courts_desktop = soup.select_one("div.hidden-xs div.label-detail:-soup-contains('Courts') + div.col-xs-11")
            if courts_desktop:
                courts = clean_text(courts_desktop.get_text(strip=True, separator=', '))
            else:
                courts_mobile = soup.select_one("div.visible-xs div.label-detail:-soup-contains('Courts') + ul.list-court")
                if courts_mobile:
                    courts = clean_text(courts_mobile.get_text(strip=True, separator=', '))
        except Exception as e:
            print(f"[WARN] Failed to extract courts for {profile_url}: {e}")

        languages = None
        try:
            lang_desktop = soup.select_one("div.col-sm-7 div.item-info span.item-label:-soup-contains('Languages') + span.value")
            if lang_desktop:
                languages = clean_text(lang_desktop.get_text(strip=True, separator=', '))
            else:
                lang_mobile = soup.select_one("div.visible-xs div.label-detail:-soup-contains('About') + div")
                if lang_mobile and 'Language(s) Spoken' in lang_mobile.get_text():
                    languages = clean_text(lang_mobile.get_text(strip=True).replace('Language(s) Spoken:', ''))
        except Exception as e:
            print(f"[WARN] Failed to extract languages for {profile_url}: {e}")

        return specialization, courts, languages
    except Exception as e:
        print(f"[ERROR] Failed parsing profile {profile_url}: {e}")
        return None, None, None

async def crawl_category_lawyers(crawler, category_name, category_url, seen_ids):
    """Crawl lawyers in a category."""
    results = []
    page = 1

    while len(results) < MAX_LAWYERS_PER_CATEGORY:
        page_url = f"{category_url}?page={page}" if page > 1 else category_url
        print(f" Crawling {page_url}")
        html = await fetch_page(crawler, page_url, "div.lawyer-item.border-box, ul.pagination a")
        if not html:
            print(f"[ERROR] Giving up crawling page {page_url}")
            break

        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("div.lawyer-item.border-box")
        if not items:
            print(f"[INFO] No more lawyers found on page {page}")
            break

        for item in items:
            if len(results) >= MAX_LAWYERS_PER_CATEGORY:
                break
            try:
                name_tag = item.select_one("h2.media-heading span[itemprop='name']")
                name = name_tag.get_text(strip=True) if name_tag else None
                loc_tag = item.select_one("span[itemprop='addressLocality']")
                location = loc_tag.get_text(strip=True) if loc_tag else None
                if not name or not location:
                    print(f"[WARN] Skipping lawyer due to missing name or location on {page_url}")
                    continue

                lawyer_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{name}-{location}"))
                if lawyer_id in seen_ids:
                    print(f"[INFO] Duplicate {name} at {location}")
                    continue
                seen_ids.add(lawyer_id)

                exp_tag = item.select_one("div.experience span")
                experience = exp_tag.get_text(strip=True) if exp_tag else None

                rating_tag = item.select_one("span.score")
                rating, user_ratings = None, None
                if rating_tag:
                    txt = rating_tag.get_text(strip=True)
                    m = re.match(r"(\d+\.\d+)\s*\|\s*(\d+\+?\s*user ratings)", txt)
                    if m:
                        rating, user_ratings = m.group(1), m.group(2)

                contact = item.select_one("a.button.button-primary.button-small")
                contact_link = urljoin(BASE_URL, contact["href"]) if contact and contact.get("href") else None

                specialization, courts, languages = None, None, None
                if contact_link:
                    specialization, courts, languages = await crawl_lawyer_profile(crawler, contact_link)
                    if specialization is None and courts is None and languages is None:
                        print(f"[WARN] Skipping lawyer {name} at {location} due to failed profile crawl")
                        continue

                answer_parts = {
                    "Address": location if location else "N/A",
                    "Experience": experience if experience else "N/A",
                    "Ratings": rating if rating else "N/A",
                    "User_Ratings": user_ratings if user_ratings else "N/A",
                    "Lawyer_type": category_name if category_name else "N/A",
                    "Specialization": specialization if specialization else "N/A",
                    "Courts": courts if courts else "N/A",
                    "Languages": languages if languages else "N/A"
                }
                answer_str = (f"Address={answer_parts['Address']}, "
                              f"Experience={answer_parts['Experience']}, "
                              f"Ratings={answer_parts['Ratings']}, "
                              f"User_Ratings={answer_parts['User_Ratings']}, "
                              f"Lawyer_type={answer_parts['Lawyer_type']}, "
                              f"Specialization={answer_parts['Specialization']}, "
                              f"Courts={answer_parts['Courts']}, "
                              f"Languages={answer_parts['Languages']}")

                lawyer_entry = {
                    "id": lawyer_id,
                    "Question": name,
                    "Answer": answer_str,
                    "url": contact_link,
                    "category_id": "Lawyer_details"
                }
                results.append(lawyer_entry)
                print(f"  Added: {name}, {answer_str}")

            except Exception as e:
                print(f"[WARN] Skipping lawyer due to parsing error on {page_url}: {e}")
                continue

        next_page_links = soup.select("ul.pagination a")
        has_next = any(a.get_text(strip=True) == "»" for a in next_page_links)
        if not has_next:
            print(f"[INFO] No more pages for {category_name}")
            break

        page += 1
        await asyncio.sleep(1)

    print(f"[DONE] {category_name} scraped {len(results)} lawyers")
    return results

async def crawl_all_categories():
    """Crawl all categories and return combined results."""
    seen_global = set()
    all_lawyers = []

    async with AsyncWebCrawler(verbose=True) as crawler:
        for cname, curl in CATEGORY_URLS.items():
            lawyers = await crawl_category_lawyers(crawler, cname, curl, seen_global)
            all_lawyers.extend(lawyers)

    return all_lawyers

@asset(name="lawyers_category_asset")
def lawyers_category_asset() -> Output[list]:
    """Dagster asset to crawl lawyer data and save to JSON."""
    scraped_data = asyncio.run(crawl_all_categories())

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)

    print(f"[DONE] Saved {len(scraped_data)} lawyers to {OUTPUT_FILE}")

    return Output(
        value=scraped_data,
        metadata={"file_path": OUTPUT_FILE, "total_lawyers": len(scraped_data)}
    )