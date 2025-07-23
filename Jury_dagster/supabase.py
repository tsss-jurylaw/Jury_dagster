import os
from dagster import asset, AssetExecutionContext
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("jury_law_supabase_url")
SUPABASE_KEY = os.getenv("jury_law_supabase_key")
SUPABASE_SCHEMA = os.getenv("jury_law_SUPABASE_SCHEMA")
SUPABASE_TABLE = os.getenv("jury_law_SUPABASE_ARTICLES_TABLE")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@asset
def supabase_articles_asset(context: AssetExecutionContext, constitution_articles: list) -> None:
    if not constitution_articles:
        context.log.warning(" No Constitution articles to insert.")
        return

    table = supabase.schema(SUPABASE_SCHEMA).table(SUPABASE_TABLE)
    inserted_count = 0

    for part in constitution_articles:
        for article in part.get("articles", []):
            details = article.get("details", {})
            try:
                res = table.insert({
                    "id": article.get("id"),
                    "part_name": part.get("part_name", ""),
                    "part_title": part.get("part_title", ""),
                    "article_range": part.get("article_range", ""),
                    "article_title": article.get("article_title", ""),
                    "article_url": article.get("article_url", ""),
                    "detail_title": details.get("title", ""),
                    "detail_text": details.get("text", ""),
                    "version_1": details.get("version_1", ""),
                    "version_2": details.get("version_2", ""),
                    "summary": details.get("summary", "")
                }).execute()
                if res.data:
                    inserted_count += 1
                    context.log.info(f" Inserted article: {article.get('article_title')}")
            except Exception as e:
                context.log.error(f" Failed to insert article {article.get('article_title')}: {e}")

    context.log.info(f" Supabase insert completed: {inserted_count} rows.")