import os
import requests
import time
from dotenv import load_dotenv
from dagster import asset

load_dotenv()

MEILISEARCH_HOST = os.getenv("MEILISEARCH_HOST")
MEILISEARCH_KEY = os.getenv("MEILISEARCH_KEY")
INDEX_NAME = os.getenv("MEILISEARCH_INDEX_NAME")

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
            print(" Timeout waiting for Meilisearch task")
            return False
        time.sleep(0.5)


@asset
def meilisearch_articles_asset(constitution_articles, ipc_sections_asset: list) -> None:
    if not all([MEILISEARCH_HOST, MEILISEARCH_KEY, INDEX_NAME]):
        print(" Missing Meilisearch environment configuration.")
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
        if not wait_for_meilisearch_task(task_id):
            return

    docs = []
    for article in constitution_articles, ipc_sections_asset:
        docs.append({
            "id": article.get("id"),
            "Question": article.get("Question", ""),
            "Answer": article.get("Answer", ""),
            "url": article.get("url", ""),
            "category": article.get("category_id", "") 
        })

    post_url = f"{MEILISEARCH_HOST}/indexes/{INDEX_NAME}/documents"
    res = requests.post(post_url, json=docs, headers=headers)
    if res.status_code in (200, 202):
        task_id = res.json().get("taskUid")
        if wait_for_meilisearch_task(task_id):
            print(f" Indexed {len(docs)} articles into Meilisearch.")
    else:
        print(" Error inserting documents:", res.text)


