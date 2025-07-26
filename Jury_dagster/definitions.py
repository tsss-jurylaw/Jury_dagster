from dagster import Definitions, load_assets_from_modules

from Jury_dagster import meilisearch, supabase

from Jury_dagster import constitution,IPC_sections

all_assets = load_assets_from_modules([

  IPC_sections,
  constitution,
  meilisearch,
  supabase

])

defs = Definitions(
    assets=all_assets,
)
