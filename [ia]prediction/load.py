from minio import Minio
from minio.error import S3Error
import pandas as pd
from io import BytesIO
import logging

# Création d’un logger spécifique à ce module
logger = logging.getLogger(__name__)

# Initialisation du client MinIO avec :
# - endpoint (localhost:9000)
# - identifiants d’accès
# - secure=False → connexion HTTP (True pour HTTPS)
client = Minio(
    "localhost:9000",
    access_key="mspr-admin",
    secret_key="4A724rhUh65XMHvVR9k73xumLhytHtm557VKC83G",
    secure=False
)

# Nom du bucket cible
bucket_name = "gold"

# FONCTION DE CHARGEMENT D’UN FICHIER PARQUET DEPUIS MINIO
def load_parquet_from_minio(object_name):
    """
    Télécharge un fichier Parquet depuis MinIO
    et le retourne sous forme de DataFrame pandas.

    :param object_name: Nom de l’objet (fichier) dans le bucket
    :return: pandas.DataFrame
    """
    try:
        logger.info(f"Téléchargement de l'objet '{object_name}' depuis le bucket '{bucket_name}'")

        # Récupération de l'objet depuis MinIO
        response = client.get_object(bucket_name, object_name)

        # Lecture du contenu binaire et conversion en DataFrame
        df = pd.read_parquet(BytesIO(response.read()))

        logger.info(f"Fichier '{object_name}' chargé avec succès ({len(df)} lignes)")

        return df

    except S3Error as e:
        # Erreur spécifique MinIO / S3
        logger.error(f"Erreur S3 lors du chargement de '{object_name}': {e}")
        raise

    except Exception as e:
        # Autres erreurs (lecture parquet, connexion, etc.)
        logger.error(f"Erreur inattendue lors du chargement de '{object_name}': {e}")
        raise

    finally:
        # Fermeture propre de la connexion si elle existe
        try:
            response.close()
            response.release_conn()
            logger.debug("Connexion MinIO fermée correctement")
        except Exception:
            pass