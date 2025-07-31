import os
from dagster import asset, AssetExecutionContext
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE")

if not all([SUPABASE_URL, SUPABASE_KEY, SUPABASE_SCHEMA, SUPABASE_TABLE]):
    raise ValueError("Missing Supabase configuration in .env file.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_uuid_map(table: str, name_field: str = "name") -> dict:
    """Fetch UUID mapping from a table based on a name field."""
    try:
        response = supabase.schema(SUPABASE_SCHEMA).table(table).select(f"id, {name_field}").execute()
        if not response.data:
            print(f"[WARN] No data in {table}")
            return {}
        return {row[name_field].strip().lower(): row["id"] for row in response.data}
    except Exception as e:
        print(f"[ERROR] get_uuid_map failed for table '{table}': {e}")
        return {}

@asset
def jury_supabase(context: AssetExecutionContext, constitution_articles, ipc_sections_asset,bns_sections_asset,hma_sections_asset,income_tax_act_sections_asset,posh_act_sections_asset,crpc_sections_asset,cpc_sections_asset,lawyers_category_asset: list) -> None:
    """Insert constitution articles, ipc_sections_asset,bns_sections_asset,hma_sections_asset,income_tax_act_sections_asset,posh_act_sections_asset,crpc_sections_asset ,cpc_sections_asset,lawyers_category_asset into Supabase with category UUID mapping."""
    if not constitution_articles + ipc_sections_asset + bns_sections_asset + hma_sections_asset + income_tax_act_sections_asset + posh_act_sections_asset + crpc_sections_asset + cpc_sections_asset + lawyers_category_asset:
        context.log.warning("No constitution_articles, ipc_sections_asset , bns_sections_asset ,hma_sections_asset, income_tax_act_sections_asset,posh_act_sections_asset, crpc_sections_asset , cpc_sections_asset,lawyers_category_asset to insert into Supabase.")
        return

    category_map = get_uuid_map("categories")

    table = supabase.schema(SUPABASE_SCHEMA).table(SUPABASE_TABLE)
    inserted_count = 0

    for section in constitution_articles, ipc_sections_asset , bns_sections_asset ,hma_sections_asset, income_tax_act_sections_asset ,posh_act_sections_asset, crpc_sections_asset , cpc_sections_asset , lawyers_category_asset:
        try:
            raw_category = section.get("category_id", "").strip().lower()
            category_uuid = category_map.get(raw_category)

            if not category_uuid:
                context.log.warning(
                    f"Skipping section {section.get('section_number', section.get('id', 'unknown'))} — unknown category '{raw_category}'"
                )
                continue

            res = table.insert({
                "id": section["id"],
                "Question": section["Question"],
                "Answer": section["Answer"],
                "url": section["url"],
                "category_id": category_uuid
            }).execute()

            if res.data:
                inserted_count += 1
                context.log.info(
                    f"Inserted section {section.get('section_number', section.get('id', 'unknown'))}"
                )

        except Exception as e:
            context.log.error(
                f"Supabase insert failed for section {section.get('section_number', section.get('id', 'unknown'))}: {e}"
            )

    context.log.info(f"Supabase insert completed: {inserted_count} rows.")



























