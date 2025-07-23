import asyncio
import json
import random
import uuid
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from playwright.async_api import async_playwright, Page, Browser
from dagster import asset, Output

BASE_URL = "https://www.constitutionofindia.net"
MAIN_URL = f"{BASE_URL}/read/"
MAX_RETRIES = 3
NAMESPACE = uuid.NAMESPACE_URL

def clean_text(text):
    return ' '.join(text.strip().split()) if text else ''


async def fetch_article_details(article_url, article_title, page: Page):
    for attempt in range(MAX_RETRIES):
        try:
            await page.goto(article_url, timeout=60000)
            await page.wait_for_selector("main#primary", timeout=15000)
            await page.wait_for_timeout(2000)
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            title_elem = soup.select_one('h1.font-title')
            title = clean_text(title_elem.text if title_elem else '')

            article_text_elem = soup.select_one('div.article-detail__intro-content')
            article_text = clean_text(article_text_elem.text if article_text_elem else '')

            version_1 = version_2 = summary = ''
            main_blocks = soup.select('div.article-detail__content__main-block')
            for block in main_blocks:
                h3 = block.select_one('h3')
                if h3:
                    h3_text = h3.text.strip().lower()
                    sub_block = block.select_one('div.article-detail__content__sub-block')
                    content = clean_text(sub_block.text if sub_block else '')
                    if h3_text == 'version 1':
                        version_1 = content
                    elif h3_text == 'version 2':
                        version_2 = content
                    elif h3_text == 'summary':
                        summary = content

            article_id = str(uuid.uuid5(NAMESPACE, article_url))

            return {
                "id": article_id,
                "article_title": article_title,
                "article_url": article_url,
                "details": {
                    "title": title,
                    "text": article_text,
                    "version_1": version_1,
                    "version_2": version_2,
                    "summary": summary
                }
            }
        except Exception as e:
            print(f" Error fetching {article_title} (Attempt {attempt+1}): {e}")
            await asyncio.sleep(2 + attempt)
    return None


@asset
def constitution_articles():
    """
    Dagster asset that scrapes the Constitution of India website using Crawl4AI (with Playwright fallback for article detail pages).
    """
    async def _scrape():
        all_parts = []

        try:
            async with AsyncWebCrawler(wait_for_selector='div[data-target="parts"]', wait_time=3000) as crawler:
                result = await crawler.arun(MAIN_URL)
                html = result.html
                if not html:
                    raise Exception("Empty main page HTML")
        except Exception as e:
            print(f" Crawl4AI failed for MAIN_URL, falling back to Playwright: {e}")
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(MAIN_URL, timeout=60000)
                await page.wait_for_selector('div[data-target="parts"]', timeout=15000)
                await page.wait_for_timeout(3000)
                html = await page.content()
                await browser.close()

        soup = BeautifulSoup(html, "lxml")
        parts_section = soup.select_one('div[data-target="parts"]')
        part_containers = parts_section.select('div.pt-12') if parts_section else []

        async with async_playwright() as p:
            browser: Browser = await p.chromium.launch(headless=True)
            page: Page = await browser.new_page()

            for i, part_container in enumerate(part_containers):
                part_data = {}

                name_elem = part_container.select_one('p.text-base.md\\:text-lg.lg\\:text-1\\.5xl')
                part_name = clean_text(name_elem.text if name_elem else '')
                part_data['part_name'] = part_name

                title_elem = part_container.select_one('h2.font-title')
                part_data['part_title'] = clean_text(title_elem.text if title_elem else '')

                range_elem = part_container.select_one('p.font-semibold')
                part_data['article_range'] = clean_text(range_elem.text if range_elem else '')

                part_data['id'] = str(uuid.uuid5(NAMESPACE, part_name))

                view_all_elem = part_container.select_one('a.uppercase.font-medium.text-xs.no-underline')
                if not view_all_elem:
                    print(f"[] Skipping {part_data['part_name']} (no 'View All')")
                    continue

                view_all_url = view_all_elem.get('href')
                view_all_url = BASE_URL + view_all_url if not view_all_url.startswith("http") else view_all_url

                print(f"\n Processing Part {i+1}: {part_data['part_name']}")
                print(f" Going to View All: {view_all_url}")

                try:
                    async with AsyncWebCrawler(wait_for_selector='ul li a', wait_time=3000) as crawler:
                        result = await crawler.arun(view_all_url)
                        view_all_html = result.html
                        if not view_all_html:
                            raise Exception("Empty View All HTML")
                except:
                    await page.goto(view_all_url, timeout=60000)
                    await page.wait_for_selector("main#primary", timeout=15000)
                    await page.wait_for_timeout(3000)
                    view_all_html = await page.content()

                soup = BeautifulSoup(view_all_html, "lxml")
                article_links = soup.select("ul li a")
                filtered_links = []
                for link in article_links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    if '/articles/' in href.lower():
                        full_url = BASE_URL + href if not href.startswith('http') else href
                        filtered_links.append((text, full_url))

                part_data["articles"] = []
                print(f"Found {len(filtered_links)} article links")
                for idx, (article_title, article_url) in enumerate(filtered_links):
                    print(f" Fetching Article {idx+1}/{len(filtered_links)}: {article_title}")
                    article_data = await fetch_article_details(article_url, article_title, page)
                    if article_data:
                        part_data["articles"].append(article_data)
                    await asyncio.sleep(random.uniform(1.0, 2.0))

                all_parts.append(part_data)

            await browser.close()
        return all_parts

    result = asyncio.run(_scrape())
    return Output(result)