import pandas as pd

raw_parquet_module = getattr(__import__("[raw]requesters.parquet"), "parquet")
raw_xls_module = getattr(__import__("[raw]requesters.xls"), "xls")
raw_melodi_module = getattr(__import__("[raw]requesters.melodi"), "melodi")

silver_dataframe_module = getattr(__import__("[silver]transformers.dataframe_cleanup"), "dataframe_cleanup")

PATHS = {
    "metadata_delinquance": "[raw]requesters/metadata/DEP_Base_statistique_delinquance_police_gendarmerie.json"
}
TEMP_PATHS = {
    "temp_delinquance": "[raw]requesters/temp/delinquance.parquet",
    "temp_taux_chommage": "[raw]requesters/temp/taux_chommage.xls"
}
URLS = {
    "delinquance": "https://object.files.data.gouv.fr/hydra-parquet/hydra-parquet/2b27a675-e3bf-41ef-a852-5fb9ab483967.parquet",
    "taux_chommage": "https://www.insee.fr/fr/statistiques/fichier/2012804/sl_etc_2025T3.xls",
    "age_moyen": "https://api.insee.fr/melodi/data/DS_RP_POPULATION_COMP?SEX=_T&PCS=_T&GEO=DEP"
}

# raw_delinquance_df = raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["delinquance"], TEMP_PATHS["temp_delinquance"], PATHS["metadata_delinquance"])
# silver_delinquance_df = silver_dataframe_module.clean_delinquance(raw_delinquance_df)
# print(silver_delinquance_df)

# raw_taux_chommage_df = raw_xls_module.creer_dataframe_depuis_xls_url(URLS["taux_chommage"], TEMP_PATHS["temp_taux_chommage"], "Département")
# silver_taux_chommage_df = silver_dataframe_module.clean_taux_chomage(raw_taux_chommage_df)
# print(silver_taux_chommage_df)

raw_age_moyen_df = raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["age_moyen"])
silver_age_moyen_df = silver_dataframe_module.clean_age_moyen(raw_age_moyen_df)
print(silver_age_moyen_df)