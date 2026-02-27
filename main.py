import pandas as pd
import json
import logging
import numpy as np

# pd.set_option('display.max_columns', None)
LOG_LEVEL = logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

raw_parquet_module = getattr(__import__("[raw]requesters.parquet"), "parquet")
raw_xls_module = getattr(__import__("[raw]requesters.xls"), "xls")
raw_melodi_module = getattr(__import__("[raw]requesters.melodi"), "melodi")
raw_mixed_xlsx_zip_module = getattr(__import__("[raw]requesters.mixedxlsxzip"), "mixedxlsxzip")
silver_dataframe_module = getattr(__import__("[silver]transformers.dataframe_cleanup"), "dataframe_cleanup")
load_quality_module = getattr(__import__("[load]loaders.quality"), "quality")
load_save_module = getattr(__import__("[load]loaders.save"), "save")

PATHS = {
    "metadata_delinquance": "[raw]requesters/metadata/DEP_Base_statistique_delinquance_police_gendarmerie.json",
    "metadata_famille_politique": "[silver]transformers/metadata/bords_politiques.json",
    "metadata_population_active": "[silver]transformers/metadata/population_active.json", 
    "metadata_categorie_professionnelle": "[silver]transformers/metadata/categorie_professionnelle.json",
    "metadata_niveau_etude": "[silver]transformers/metadata/niveau_etude.json"
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
    "etablissement_culturel": "https://data.culture.gouv.fr/api/explore/v2.1/catalog/datasets/entreprises-culturelles-par-departement/exports/parquet?lang=fr&timezone=Europe%2FBerlin",
    "pouvoir_achat": "https://www.insee.fr/fr/outil-interactif/5367857/data/30_RPC/31_RNP/31H_Figure8/dataExcel.fr.xlsx",
    "niveau_etude": "https://api.insee.fr/melodi/data/DS_RP_DIPLOMES_PRINC?SEX=_T&EDUC=001T100_RP&EDUC=001T003_RP&EDUC=001T200_RP&EDUC=100_RP&EDUC=200_RP&EDUC=300_RP&EDUC=700_RP&EDUC=600T702_RP&EDUC=600_RP&EDUC=500T702_RP&EDUC=350T351_RP&EDUC=500_RP&GEO=DEP",
    "abstention_votant": "https://object.files.data.gouv.fr/hydra-parquet/hydra-parquet/b8703c69-a18f-46ab-9e7f-3a8368dcb891.parquet",
    "president_sortant": "https://object.files.data.gouv.fr/data-pipeline-open/elections/candidats_results.parquet"
}

dataframes = {}
# dataframes["silver_delinquance_df"] = silver_dataframe_module.clean_delinquance(raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["delinquance"], PATHS["metadata_delinquance"]))
# dataframes["silver_taux_chommage_df"] = silver_dataframe_module.clean_taux_chomage(raw_xls_module.creer_dataframe_depuis_xls_url(URLS["taux_chommage"], "Département"))
# dataframes["silver_age_moyen_df"] = silver_dataframe_module.clean_age_moyen(raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["age_moyen"]))
# dataframes["silver_population_active_df"] = silver_dataframe_module.clean_population_active(raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["population_active"]), PATHS["metadata_population_active"])
# dataframes["silver_categorie_professionnelle_df"] = silver_dataframe_module.clean_categorie_professionnelle(raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["categorie_professionnelle"]), PATHS["metadata_categorie_professionnelle"])
# dataframes["silver_equipement_sportif_df"] = silver_dataframe_module.clean_equipement_sportif(raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["equipement_sportif"], {}))
# dataframes["silver_revenu_moyen_df"] = silver_dataframe_module.clean_revenu_moyen(raw_mixed_xlsx_zip_module.creer_dataframe_depuis_multiple_url(URLS["revenu_moyen"]))
# dataframes["silver_etablissement_culturel_df"] = silver_dataframe_module.clean_etablissement_culturel(raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["etablissement_culturel"], {}))
# dataframes["silver_pouvoir_achat_df"] = silver_dataframe_module.clean_pouvoir_achat(raw_xls_module.creer_dataframe_depuis_xls_url(URLS["pouvoir_achat"], "Données"))
# dataframes["silver_niveau_etude_df"] = silver_dataframe_module.clean_niveau_etude(raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["niveau_etude"]), PATHS["metadata_niveau_etude"])
# dataframes["silver_abstention_votant_df"] = silver_dataframe_module.clean_abstention_votant(raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["abstention_votant"], {}))
dataframes["silver_president_sortant_df"] = silver_dataframe_module.clean_president_sortant(raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["president_sortant"], {}), PATHS["metadata_famille_politique"])

load_save_module.save_all_silver_dataframes(dataframes)
load_quality_module.audit_all_silver_dataframes(dataframes)