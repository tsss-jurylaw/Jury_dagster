from dagster import Definitions, load_assets_from_modules

from Jury_dagster import meilisearch, supabase

from Jury_dagster import constitution

all_assets = load_assets_from_modules([


  constitution,
  meilisearch,
  supabase

])

defs = Definitions(
    assets=all_assets,
)
