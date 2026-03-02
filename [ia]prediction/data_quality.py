import pandas as pd
import json
import logging
from minio import Minio
from io import BytesIO
from datetime import datetime
from minio.error import S3Error

# Configuration du logging
logger = logging.getLogger(__name__)

def upload_json_to_minio(
    data,
    bucket_name="ia-data",
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

def quality_report(df, name, object_name):
    report = {}
    report["dataset"] = name
    report["rows"] = int(len(df))
    report["columns"] = int(len(df.columns))
    report["null_values"] = {k: int(v) for k, v in df.isnull().sum().items()}
    report["duplicate_rows"] = int(df.duplicated().sum())
    report["dtypes"] = df.dtypes.astype(str).to_dict()

    upload_json_to_minio(
        data=report,
        bucket_name="ia-data",
        object_name=object_name
    ),
    