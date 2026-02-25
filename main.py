import pandas as pd
import json

# pd.set_option('display.max_columns', None)

raw_parquet_module = getattr(__import__("[raw]requesters.parquet"), "parquet")
raw_xls_module = getattr(__import__("[raw]requesters.xls"), "xls")
raw_melodi_module = getattr(__import__("[raw]requesters.melodi"), "melodi")
raw_mixed_xlsx_zip_module = getattr(__import__("[raw]requesters.mixedxlsxzip"), "mixedxlsxzip")

silver_dataframe_module = getattr(__import__("[silver]transformers.dataframe_cleanup"), "dataframe_cleanup")

PATHS = {
    "metadata_delinquance": "[raw]requesters/metadata/DEP_Base_statistique_delinquance_police_gendarmerie.json",
    "metadata_famille_politique": "[silver]transformers/metadata/bords_politiques.json",
    "metadata_population_active": "[silver]transformers/metadata/population_active.json", 
    "metadata_categorie_professionnelle": "[silver]transformers/metadata/categorie_professionnelle.json"
}
URLS = {
    "delinquance": "https://object.files.data.gouv.fr/hydra-parquet/hydra-parquet/2b27a675-e3bf-41ef-a852-5fb9ab483967.parquet",
    "taux_chommage": "https://www.insee.fr/fr/statistiques/fichier/2012804/sl_etc_2025T3.xls",
    "age_moyen": "https://api.insee.fr/melodi/data/DS_RP_POPULATION_COMP?SEX=_T&PCS=_T&GEO=DEP",
    "population_active": "https://api.insee.fr/melodi/data/DS_RP_EMPLOI_LR_PRINC?SEX=_T&EDUC=_T&EMPSTA_ENQ=1&EMPSTA_ENQ=31&EMPSTA_ENQ=33&EMPSTA_ENQ=35&EMPSTA_ENQ=36&GEO=DEP",
    "categorie_professionnelle": "https://api.insee.fr/melodi/data/DD_EEC_SERIES?UNIT_MEASURE=_Z&SEX=_T&AGE=_T&EDUC=_T&WKTIME=_T&UNDEREMP=_T",
    "equipement_sportif": "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-data-es-base-de-donnees/exports/parquet?lang=fr&timezone=Europe%2FBerlin",
    "revenu_moyen": {
        "8420": "https://static.data.gouv.fr/resources/limpot-sur-le-revenu-par-collectivite-territoriale-ircom/20230228-115014/impotrevenudep-1984-2020.xlsx",
        "21": "https://static.data.gouv.fr/resources/limpot-sur-le-revenu-par-collectivite-territoriale-ircom/20230608-145433/ircom-2022-revenus-2021.zip",
        "22": "https://static.data.gouv.fr/resources/limpot-sur-le-revenu-par-collectivite-territoriale-ircom/20240610-082154/ircom-2023-revenus-2022.zip",
        "23": "https://static.data.gouv.fr/resources/limpot-sur-le-revenu-par-collectivite-territoriale-ircom/20250919-135242/ircom-2024-revenus-2023.zip"
    },
    "president_sortant": "https://object.files.data.gouv.fr/data-pipeline-open/elections/candidats_results.parquet"
}

# raw_delinquance_df = raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["delinquance"], PATHS["metadata_delinquance"])
# silver_delinquance_df = silver_dataframe_module.clean_delinquance(raw_delinquance_df)
# print(silver_delinquance_df)

# raw_taux_chommage_df = raw_xls_module.creer_dataframe_depuis_xls_url(URLS["taux_chommage"], "Département")
# silver_taux_chommage_df = silver_dataframe_module.clean_taux_chomage(raw_taux_chommage_df)
# print(silver_taux_chommage_df)

# raw_age_moyen_df = raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["age_moyen"])
# silver_age_moyen_df = silver_dataframe_module.clean_age_moyen(raw_age_moyen_df)
# print(silver_age_moyen_df)

# raw_population_active_df = raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["population_active"])
# silver_population_active_df = silver_dataframe_module.clean_population_active(raw_population_active_df, PATHS["metadata_population_active"])
# print(silver_population_active_df)

# raw_categorie_professionnelle_df = raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["categorie_professionnelle"])
# silver_categorie_professionnelle_df = silver_dataframe_module.clean_categorie_professionnelle(raw_categorie_professionnelle_df, PATHS["metadata_categorie_professionnelle"])
# print(silver_categorie_professionnelle_df)

# raw_equipement_sportif_df = raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["equipement_sportif"], {})
# silver_equipement_sportif_df = silver_dataframe_module.clean_equipement_sportif(raw_equipement_sportif_df)
# print(silver_equipement_sportif_df)

# raw_revenu_moyen_dfs = raw_mixed_xlsx_zip_module.creer_dataframe_depuis_multiple_url(URLS["revenu_moyen"])
# silver_revenu_moyen_df = silver_dataframe_module.clean_revenu_moyen(raw_revenu_moyen_dfs)
# print(silver_revenu_moyen_df)



# raw_president_sortant_df = raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["president_sortant"], {})
# silver_president_sortant_df = silver_dataframe_module.clean_president_sortant(raw_president_sortant_df, PATHS["metadata_famille_politique"])
# print(silver_president_sortant_df)