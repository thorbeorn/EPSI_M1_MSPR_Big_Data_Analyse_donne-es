import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import logging
import time

# CONFIGURATION DU LOGGING
logger = logging.getLogger("gold-layer")
# CONNEXION BASE DE DONNÉES
engine = create_engine(
    "mysql+pymysql://mspr-user:********@localhost:3306/mspr-db"
)

# FONCTION GÉNÉRIQUE : UPLOAD DATAFRAME → MINIO
def upload_df_to_minio(
    df: pd.DataFrame,
    file_format: str,  # "csv" ou "parquet"
    bucket_name="data-lake",
    object_name=None,
    endpoint="localhost:9000",
    access_key="mspr-admin",
    secret_key="********"
):
    """
    Upload un DataFrame Pandas en CSV ou Parquet directement dans MinIO
    sans création de fichier local.
    """

    logger.debug(f"Début upload vers bucket '{bucket_name}' au format {file_format}")

    if file_format not in ["csv", "parquet"]:
        raise ValueError("file_format doit être 'csv' ou 'parquet'")

    # Génération d’un nom horodaté si non fourni
    if object_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"export_{timestamp}.{file_format}"

    try:
        start_time = time.time()

        buffer = BytesIO()

        # Conversion du DataFrame
        logger.debug("Conversion du DataFrame en mémoire")

        if file_format == "csv":
            df.to_csv(buffer, index=False)
            content_type = "text/csv"

        elif file_format == "parquet":
            df.to_parquet(buffer, index=False, engine="pyarrow")
            content_type = "application/octet-stream"

        buffer.seek(0)

        # Initialisation client MinIO
        logger.debug("Connexion au serveur MinIO")

        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        # Création du bucket si inexistant
        if not client.bucket_exists(bucket_name):
            logger.warning(f"Bucket '{bucket_name}' inexistant → création")
            client.make_bucket(bucket_name)

        # Upload objet
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type=content_type
        )

        duration = round(time.time() - start_time, 2)

        logger.debug(
            f"Upload réussi : {bucket_name}/{object_name} "
            f"| lignes={len(df)} "
            f"| taille={buffer.getbuffer().nbytes} bytes "
            f"| durée={duration}s"
        )

    except S3Error as err:
        logger.error(f"Erreur MinIO : {err}")
        raise

    except Exception as e:
        logger.exception(f"Erreur inattendue lors de l'upload : {e}")
        raise


# GOLD LAYER : INDICATEURS COMPLETS
def create_gold_all_indicator_df():
    """
    Construit la table GOLD consolidée des indicateurs
    (jointure multi-tables département + année)
    puis exporte en CSV et Parquet vers MinIO.
    """

    logger.info("Création dataset GOLD - all_indicator")

    query = """ 
    -- Requête SQL consolidée multi-indicateurs
    SELECT 
        base.Code_departement,
        base.annee,
        ...
    FROM indicateurs base
    LEFT JOIN age_moyen am 
        ON am.Code_departement = base.Code_departement 
        AND am.annee = base.annee
    ...
    """

    try:
        start_time = time.time()

        logger.debug("Exécution requête SQL indicateurs")
        df = pd.read_sql(query, engine)

        logger.debug(f"Requête exécutée | lignes récupérées : {len(df)}")

        # Upload CSV
        upload_df_to_minio(
            df,
            file_format="csv",
            bucket_name="gold",
            object_name="all_indicator.csv"
        )

        # Upload Parquet
        upload_df_to_minio(
            df,
            file_format="parquet",
            bucket_name="gold",
            object_name="all_indicator.parquet"
        )

        duration = round(time.time() - start_time, 2)
        logger.info(f"Dataset all_indicator terminé en {duration}s")

    except Exception as e:
        logger.exception(f"Erreur lors de la création du GOLD indicateur : {e}")
        raise

# GOLD LAYER : PRESIDENT SORTANT (VAINQUEUR T2)
def create_gold_all_president_df():
    """
    Construit la table GOLD des présidents gagnants au second tour
    (max nombre de voix par département et année).
    """

    logger.info("Création dataset GOLD - all_president")

    query = """ 
    SELECT p.*
    FROM president_sortant p
    JOIN (
        SELECT code_departement,
            annee,
            MAX(`[president_sortant]nombre_voix`) AS max_voix
        FROM president_sortant
        WHERE `[president_sortant]tour` = 't2'
        GROUP BY code_departement, annee
    ) m
    ON p.code_departement = m.code_departement
    AND p.annee = m.annee
    AND p.`[president_sortant]nombre_voix` = m.max_voix
    WHERE p.`[president_sortant]tour` = 't2';
    """

    try:
        start_time = time.time()

        logger.debug("Exécution requête SQL président")
        df = pd.read_sql(query, engine)

        logger.debug(f"Requête exécutée | lignes récupérées : {len(df)}")

        # Upload CSV
        upload_df_to_minio(
            df,
            file_format="csv",
            bucket_name="gold",
            object_name="all_president.csv"
        )

        # Upload Parquet
        upload_df_to_minio(
            df,
            file_format="parquet",
            bucket_name="gold",
            object_name="all_president.parquet"
        )

        duration = round(time.time() - start_time, 2)
        logger.info(f"Dataset all_president terminé en {duration}s")

    except Exception as e:
        logger.exception(f"Erreur lors de la création du GOLD president : {e}")
        raise