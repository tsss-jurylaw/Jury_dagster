import os
import requests
import time
from dotenv import load_dotenv
from dagster import asset

load_dotenv()

MEILISEARCH_HOST = os.getenv("MEILISEARCH_HOST")
MEILISEARCH_KEY = os.getenv("MEILISEARCH_KEY")
INDEX_NAME = os.getenv("MEILISEARCH_ARTICLES_INDEX_NAME")

headers = {
    "Authorization": f"Bearer {MEILISEARCH_KEY}",
    "Content-Type": "application/json"
}

def wait_for_meilisearch_task(task_uid: int, timeout: int = 10) -> bool:
    task_url = f"{MEILISEARCH_HOST}/tasks/{task_uid}"
    start = time.time()
    while True:
        resp = requests.get(task_url, headers=headers)
        if resp.status_code != 200:
            print(f" HTTP {resp.status_code}: {resp.text}")
            return False
        status = resp.json().get("status")
        if status == "succeeded":
            return True
        if status == "failed":
            print(f" Task {task_uid} failed:", resp.json())
            return False
        if time.time() - start > timeout:
            print(" Timeout")
            return False
        time.sleep(0.5)

@asset
def meilisearch_articles_asset(constitution_articles: list) -> None:
    if not all([MEILISEARCH_HOST, MEILISEARCH_KEY, INDEX_NAME]):
        print(" Missing Meilisearch env config")
        return

    index_url = f"{MEILISEARCH_HOST}/indexes/{INDEX_NAME}"
    check = requests.get(index_url, headers=headers)
    if check.status_code == 404:
        create_resp = requests.post(
            f"{MEILISEARCH_HOST}/indexes",
            json={"uid": INDEX_NAME, "primaryKey": "id"},
            headers=headers
        )
        task_id = create_resp.json().get("taskUid")
        if not wait_for_meilisearch_task(task_id): return

    docs = []
    for part in constitution_articles:
        for art in part["articles"]:
            details = art.get("details", {})
            docs.append({
                "id": art["id"],
                "part_name": part["part_name"],
                "part_title": part["part_title"],
                "article_range": part["article_range"],
                "article_title": art["article_title"],
                "article_url": art["article_url"],
                "title": details.get("title", ""),
                "text": details.get("text", ""),
                "version_1": details.get("version_1", ""),
                "version_2": details.get("version_2", ""),
                "summary": details.get("summary", "")
            })

    post_url = f"{MEILISEARCH_HOST}/indexes/{INDEX_NAME}/documents"
    res = requests.post(post_url, json=docs, headers=headers)
    if res.status_code in (200, 202):
        task_id = res.json().get("taskUid")
        if wait_for_meilisearch_task(task_id):
            print(f" Indexed {len(docs)} articles.")
    else:
        print(" Error inserting docs:", res.text)