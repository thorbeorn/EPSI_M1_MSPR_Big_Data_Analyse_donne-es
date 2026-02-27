import pandas as pd
import json
import logging
from minio import Minio
from io import BytesIO
from datetime import datetime
from minio.error import S3Error

# Configuration du logging
logger = logging.getLogger(__name__)

# Fonction : Upload JSON en mémoire vers MinIO
def upload_json_to_minio(
    data,
    bucket_name="data-quality",
    object_name=None,
    endpoint="localhost:9000",
    access_key="mspr-admin",
    secret_key="4A724rhUh65XMHvVR9k73xumLhytHtm557VKC83G"
):
    """
    Envoie un objet Python (dict ou list) sous forme JSON directement
    dans MinIO sans création de fichier local.

    Paramètres :
    - data : données Python à convertir en JSON
    - bucket_name : nom du bucket cible
    - object_name : nom du fichier dans MinIO (auto si None)
    - endpoint : adresse du serveur MinIO
    """

    # Génération d’un nom horodaté si non fourni
    if object_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"data_quality_report_{timestamp}.json"

    logger.debug(f"Préparation de l'upload vers MinIO : {bucket_name}/{object_name}")

    try:
        # Conversion des données en JSON puis en bytes
        json_bytes = json.dumps(data, indent=4, ensure_ascii=False).encode("utf-8")
        json_buffer = BytesIO(json_bytes)

        # Initialisation du client MinIO
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # HTTP en local
        )

        # Création du bucket s'il n'existe pas
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket créé : {bucket_name}")

        # Upload depuis la mémoire
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=json_buffer,
            length=len(json_bytes),
            content_type="application/json"
        )

        logger.debug(f"Upload réussi vers MinIO : {bucket_name}/{object_name}")

    except S3Error as err:
        logger.error(f"Erreur MinIO : {err}")
    except Exception as e:
        logger.exception(f"Erreur inattendue lors de l'upload : {e}")

# Fonction : Audit d’un DataFrame
def audit_dataframe(df, df_name):
    """
    Analyse la qualité d’un DataFrame :
    - taille
    - doublons
    - valeurs manquantes
    - valeurs négatives (numériques)
    - score global de qualité
    """

    logger.debug(f"Début audit du DataFrame : {df_name}")

    report = {}

    # Informations générales
    report["dataframe_name"] = df_name
    report["nb_rows"] = len(df)
    report["nb_columns"] = len(df.columns)
    report["duplicates"] = int(df.duplicated().sum())

    report["columns"] = {}

    total_missing_percent = 0
    numeric_columns_checked = 0

    # Analyse colonne par colonne
    for col in df.columns:
        col_data = df[col]

        missing_count = col_data.isnull().sum()
        missing_percent = col_data.isnull().mean() * 100

        column_report = {
            "dtype": str(col_data.dtype),
            "missing_values": int(missing_count),
            "missing_percent": round(float(missing_percent), 2),
            "unique_values": int(col_data.nunique())
        }

        # Vérification des valeurs négatives pour les colonnes numériques
        if pd.api.types.is_numeric_dtype(col_data):
            numeric_columns_checked += 1
            negative_values = int((col_data < 0).sum())
            column_report["negative_values"] = negative_values

        report["columns"][col] = column_report
        total_missing_percent += missing_percent

    # Calcul du score qualité global
    avg_missing = total_missing_percent / len(df.columns) if len(df.columns) > 0 else 0
    duplicate_penalty = report["duplicates"] / len(df) * 100 if len(df) > 0 else 0

    quality_score = 100 - avg_missing - duplicate_penalty
    quality_score = max(0, round(quality_score, 2))

    report["quality_score"] = quality_score

    logger.debug(
        f"Audit terminé pour {df_name} | "
        f"Lignes: {report['nb_rows']} | "
        f"Score qualité: {quality_score}"
    )

    return report

# Fonction : Audit de tous les DataFrames silver_* du dictionnaire donné
def audit_all_silver_dataframes(namespace):
    """
    Recherche tous les DataFrames dont le nom commence par 'silver_',
    lance l’audit sur chacun et envoie le rapport global dans MinIO.
    """

    logger.info("Début de l'audit global des DataFrames Silver")

    reports = []
    dataframe_count = 0

    for var_name, var_value in namespace.items():
        # Filtre : uniquement les DataFrames Silver
        if var_name.startswith("silver_") and isinstance(var_value, pd.DataFrame):
            logger.debug(f"Audit en cours : {var_name}")
            report = audit_dataframe(var_value, var_name)
            reports.append(report)
            dataframe_count += 1

    if dataframe_count == 0:
        logger.warning("Aucun DataFrame Silver trouvé dans le namespace")
    else:
        logger.debug(f"{dataframe_count} DataFrame(s) audité(s)")

    # Envoi du rapport consolidé vers MinIO
    logger.debug("Envoi du rapport global vers MinIO")
    upload_json_to_minio(
        data=reports,
        bucket_name="data-quality"
    )

    logger.info("Audit global terminé")

    return reports