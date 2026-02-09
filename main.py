import pandas as pd

raw_requesters = __import__("[raw]requesters.parquet")
raw_parquet_module = getattr(raw_requesters, "parquet")

raw_transformer = __import__("[raw]transformers.dataframe")
raw_dataframe_module = getattr(raw_transformer, "dataframe")

TEMP_PATHS = {
    "temp_delinquance": "[raw]requesters/temp/delinquance.parquet"
}
PATHS = {
    "metadata_delinquance": "[raw]requesters/metadata/DEP_Base_statistique_delinquance_police_gendarmerie.json"
}
URLS = {
    "delinquance": "https://object.files.data.gouv.fr/hydra-parquet/hydra-parquet/2b27a675-e3bf-41ef-a852-5fb9ab483967.parquet"
}

raw_delinquance_df = raw_parquet_module.creer_dataframe_depuis_parquet(URLS["delinquance"], TEMP_PATHS["temp_delinquance"], PATHS["metadata_delinquance"])
silver_delinquance_df = raw_dataframe_module.agregation_delinquance_departement(raw_delinquance_df)
print(silver_delinquance_df.head())
print(silver_delinquance_df.attrs["metadata"])
print(len(silver_delinquance_df))