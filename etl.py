import pandas as pd
import json
import logging
import numpy as np

# CONFIGURATION DU LOGGING
LOG_LEVEL = logging.DEBUG

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("ETL_PIPELINE")

logger.info("Démarrage du pipeline ETL")

# IMPORT DYNAMIQUE DES MODULES
logger.debug("Chargement des modules dynamiques")

raw_parquet_module = getattr(__import__("[raw]requesters.parquet"), "parquet")
raw_csv_module = getattr(__import__("[raw]requesters.csv"), "csv")
raw_xls_module = getattr(__import__("[raw]requesters.xls"), "xls")
raw_melodi_module = getattr(__import__("[raw]requesters.melodi"), "melodi")
raw_mixed_xlsx_zip_module = getattr(__import__("[raw]requesters.mixedxlsxzip"), "mixedxlsxzip")
silver_dataframe_module = getattr(__import__("[silver]transformers.dataframe_cleanup"), "dataframe_cleanup")
load_quality_module = getattr(__import__("[load]loaders.quality"), "quality")
load_save_module = getattr(__import__("[load]loaders.save"), "save")
gold_dwh_module = getattr(__import__("[gold]dashboards.dwh"), "dwh")

logger.debug("Modules chargés avec succès")

# CONSTANTES : PATHS & URLS
PATHS = {
    "metadata_delinquance": "[raw]requesters/metadata/DEP_Base_statistique_delinquance_police_gendarmerie.json",
    "metadata_famille_politique": "[silver]transformers/metadata/bords_politiques.json",
    "metadata_population_active": "[silver]transformers/metadata/population_active.json", 
    "metadata_categorie_professionnelle": "[silver]transformers/metadata/categorie_professionnelle.json",
    "metadata_niveau_etude": "[silver]transformers/metadata/niveau_etude.json"
}

URLS = {
    "delinquance": "https://object.files.data.gouv.fr/hydra-parquet/hydra-parquet/2b27a675-e3bf-41ef-a852-5fb9ab483967.parquet",
    "taux_chommage": "https://www.insee.fr/fr/statistiques/fichier/2012804/sl_etc_2025T4.xls",
    "age_moyen": "https://data.sports.gouv.fr/api/explore/v2.1/catalog/datasets/pop_dep_age_sexe/exports/parquet?lang=fr&timezone=Europe%2FBerlin",
    "compte_publique": "https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-departements-fonctionnelle/exports/parquet?lang=fr&timezone=Europe%2FBerlin",
    "categorie_professionnelle": "https://api.insee.fr/melodi/data/DD_EEC_SERIES?UNIT_MEASURE=_Z&SEX=_T&AGE=_T&EDUC=_T&WKTIME=_T&UNDEREMP=_T",
    "equipement_sportif": "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-data-es-base-de-donnees/exports/parquet?lang=fr&timezone=Europe%2FBerlin",
    "professionnels_sante": {
        "16": "https://www.assurance-maladie.ameli.fr/sites/default/files/2016_effectif-densite-de-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls",
        "17": "https://www.assurance-maladie.ameli.fr/sites/default/files/2017_effectif-densite-de-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls",
        "18": "https://www.assurance-maladie.ameli.fr/sites/default/files/2018_effectif-densite-de-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls",
        "19": "https://www.assurance-maladie.ameli.fr/sites/default/files/2019_effectif-densite-des-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls",
        "20": "https://www.assurance-maladie.ameli.fr/sites/default/files/2020_effectif-densite-des-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls",
        "21": "https://www.assurance-maladie.ameli.fr/sites/default/files/2021_effectif-densite-des-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls",
        "22": "https://www.assurance-maladie.ameli.fr/sites/default/files/2022_effectif-densite-des-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls",
        "23": "https://www.assurance-maladie.ameli.fr/sites/default/files/2023_effectif-densite-des-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls",
        "24": "https://www.assurance-maladie.ameli.fr/sites/default/files/2024_effectif-densite-des-professionnels-de-sante-liberaux-par-departement_serie-annuelle.xls"
    },
    "etablissement_culturel": "https://data.culture.gouv.fr/api/explore/v2.1/catalog/datasets/entreprises-culturelles-par-departement/exports/parquet?lang=fr&timezone=Europe%2FBerlin",
    "etablissement_culturel_2024": "https://www.insee.fr/fr/statistiques/fichier/8217527/DS_BPE_SPORT_CULTURE_CSV_FR.zip",
    "pouvoir_achat": "https://www.insee.fr/fr/statistiques/fichier/2830166/reve-niv-vie-pouv-achat-trim.xlsx",
    "niveau_etude": "https://api.insee.fr/melodi/data/DS_RP_DIPLOMES_PRINC?SEX=_T&EDUC=001T100_RP&EDUC=001T003_RP&EDUC=001T200_RP&EDUC=100_RP&EDUC=200_RP&EDUC=300_RP&EDUC=700_RP&EDUC=600T702_RP&EDUC=600_RP&EDUC=500T702_RP&EDUC=350T351_RP&EDUC=500_RP&GEO=DEP",
    "niveau_etude_2024": "https://www.insee.fr/fr/statistiques/fichier/8612520/FPORSOC25-F8.xlsx",
    "president_sortant": "https://object.files.data.gouv.fr/data-pipeline-open/elections/candidats_results.parquet"
}

# HELPER POUR STANDARDISER LES LOGS
def log_dataframe_info(df_name, df):
    logger.debug(
        f"{df_name} chargé | shape={df.shape} | colonnes={len(df.columns)}"
    )


# ETAPE SILVER : EXTRACTION + TRANSFORMATION
logger.debug("Début étape SILVER")

dataframes = {}

try:
    logger.debug("Traitement : delinquance")
    dataframes["silver_delinquance_df"] = silver_dataframe_module.clean_delinquance(
        raw_parquet_module.creer_dataframe_depuis_parquet_url(
            URLS["delinquance"],
            PATHS["metadata_delinquance"]
        )
    )
    log_dataframe_info("silver_delinquance_df", dataframes["silver_delinquance_df"])

    logger.debug("Traitement : taux_chommage")
    dataframes["silver_taux_chommage_df"] = silver_dataframe_module.clean_taux_chomage(
        raw_xls_module.creer_dataframe_depuis_xls_url(
            URLS["taux_chommage"],
            "Département"
        )
    )
    log_dataframe_info("silver_taux_chommage_df", dataframes["silver_taux_chommage_df"])

    logger.debug("Traitement : age_moyen")
    dataframes["silver_age_moyen_df"] = silver_dataframe_module.clean_age_moyen(
        raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["age_moyen"], {})
    )
    log_dataframe_info("silver_age_moyen_df", dataframes["silver_age_moyen_df"])

    logger.debug("Traitement : compte_publique")
    dataframes["silver_compte_publique_df"] = silver_dataframe_module.clean_compte_publique(
        raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["compte_publique"], {})
    )
    log_dataframe_info("silver_compte_publique_df", dataframes["silver_compte_publique_df"])

    logger.debug("Traitement : categorie_professionnelle")
    dataframes["silver_categorie_professionnelle_df"] = silver_dataframe_module.clean_categorie_professionnelle(
        raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["categorie_professionnelle"]),
        PATHS["metadata_categorie_professionnelle"]
    )
    log_dataframe_info("silver_categorie_professionnelle_df", dataframes["silver_categorie_professionnelle_df"])

    logger.debug("Traitement : equipement_sportif")
    dataframes["silver_equipement_sportif_df"] = silver_dataframe_module.clean_equipement_sportif(
        raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["equipement_sportif"], {})
    )
    log_dataframe_info("silver_equipement_sportif_df", dataframes["silver_equipement_sportif_df"])

    logger.debug("Traitement : professionnels_sante")
    dataframes["silver_professionnels_sante_df"] = silver_dataframe_module.clean_professionnels_sante(
        raw_mixed_xlsx_zip_module.creer_dataframe_depuis_multiple_url(URLS["professionnels_sante"])
    )
    log_dataframe_info("silver_professionnels_sante_df", dataframes["silver_professionnels_sante_df"])

    logger.debug("Traitement : etablissement_culturel")
    dataframes["silver_etablissement_culturel_df"] = silver_dataframe_module.clean_etablissement_culturel(
        raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["etablissement_culturel"], {}),
        raw_csv_module.creer_dataframe_depuis_csv_url(URLS["etablissement_culturel_2024"])
    )
    log_dataframe_info("silver_etablissement_culturel_df", dataframes["silver_etablissement_culturel_df"])

    logger.debug("Traitement : pouvoir_achat")
    dataframes["silver_pouvoir_achat_df"] = silver_dataframe_module.clean_pouvoir_achat(
        raw_xls_module.creer_dataframe_depuis_xls_url(URLS["pouvoir_achat"], "Données")
    )
    log_dataframe_info("silver_pouvoir_achat_df", dataframes["silver_pouvoir_achat_df"])

    logger.debug("Traitement : niveau_etude")
    dataframes["silver_niveau_etude_df"] = silver_dataframe_module.clean_niveau_etude(
        raw_melodi_module.creer_dataframe_depuis_melodi_api_url(URLS["niveau_etude"]),
        raw_xls_module.creer_dataframe_depuis_xls_url(URLS["niveau_etude_2024"], "Figure 1"),
        PATHS["metadata_niveau_etude"]
    )
    log_dataframe_info("silver_niveau_etude_df", dataframes["silver_niveau_etude_df"])

    logger.debug("Traitement : president_sortant")
    dataframes["silver_president_sortant_df"] = silver_dataframe_module.clean_president_sortant(
        raw_parquet_module.creer_dataframe_depuis_parquet_url(URLS["president_sortant"], {}),
        PATHS["metadata_famille_politique"]
    )
    log_dataframe_info("silver_president_sortant_df", dataframes["silver_president_sortant_df"])
    
except Exception as e:
    logger.exception("Erreur durant l'étape SILVER")
    raise

logger.debug("Fin étape SILVER")

# ETAPE LOAD
try:
    logger.debug("Sauvegarde des dataframes SILVER")
    load_save_module.save_all_silver_dataframes(dataframes)

    logger.debug("Audit qualité des dataframes SILVER")
    load_quality_module.audit_all_silver_dataframes(dataframes)

except Exception:
    logger.exception("Erreur durant l'étape LOAD")
    raise

# ETAPE GOLD
try:
    logger.debug("Création GOLD - indicateurs")
    gold_dwh_module.create_gold_all_indicator_df()

    logger.debug("Création GOLD - président")
    gold_dwh_module.create_gold_all_president_df()

except Exception:
    logger.exception("Erreur durant l'étape GOLD")
    raise

logger.info("Pipeline ETL terminé avec succès")