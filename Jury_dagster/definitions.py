
from dagster import Definitions, load_assets_from_modules

from Jury_dagster import meilisearch, supabase

from Jury_dagster import  constitution,IPC_sections,BNS_sections,HMA_sections,Income_tax,posh,CrPC,CPC_sections,Lawyer_details,legal_queries
all_assets = load_assets_from_modules([


  constitution,
  IPC_sections,
  BNS_sections,
  HMA_sections,
  Income_tax,
  posh,
  CrPC,
  CPC_sections,
  Lawyer_details,
  legal_queries,
  meilisearch,
  supabase

])

defs = Definitions(
    assets=all_assets,
)
