import pandas as pd
import os
import requests
import tempfile
import logging
import urllib3
import zipfile

# Set le logger pour les logs
logger = logging.getLogger(__name__)

# Désactivation du warning externe pour le certificat
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


"""
Télécharge un fichier csv depuis une URL.

Cette fonction appartient à la couche RAW de l’ETL :
- Téléchargement du fichier distant
- Lecture brute en DataFrame
- Ajout de métadonnées (sans transformation métier)

Parameters
----------
csv_url : str
    URL du fichier csv à télécharger

Returns
-------
pd.DataFrame
    DataFrame contenant les données csv
"""
def creer_dataframe_depuis_csv_url(csv_url: str) -> pd.DataFrame:

    logger.info("Début du chargement du fichier csv depuis URL")

    try:
        logger.debug(f"Téléchargement du fichier depuis : {csv_url}")

        response = requests.get(csv_url, verify=False)
        response.raise_for_status()

        # Cas 1 : fichier ZIP
        if csv_url.lower().endswith(".zip"):

            logger.debug("Fichier zip détecté")

            tmp_zip = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)

            try:
                tmp_zip.write(response.content)
                tmp_zip.close()

                logger.debug("Ouverture de l'archive zip")

                with zipfile.ZipFile(tmp_zip.name, "r") as zip_ref:
                    logger.debug("Liste des fichiers dans l'archive :")
                    for name in zip_ref.namelist():
                        logger.debug(name)

                    target_file = None

                    for name in zip_ref.namelist():
                        if "DS_BPE_SPORT_CULTURE_2024_data.csv" in name:
                            target_file = name
                            break
                    if target_file is None:
                        raise Exception(
                            "DS_BPE_SPORT_CULTURE_2024_data.csv introuvable dans l'archive"
                        )

                    logger.debug(f"Fichier trouvé dans le zip : {target_file}")
                    with zip_ref.open(target_file) as csv_file:
                        df = pd.read_csv(csv_file, sep=";", dtype={"GEO": "string"})

            finally:
                logger.debug("Suppression du fichier zip temporaire")
                os.remove(tmp_zip.name)

        # Cas 2 : fichier CSV simple
        else:
            logger.debug("Fichier csv détecté")
            tmp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)

            try:
                tmp_file.write(response.content)
                tmp_file.close()
                logger.debug(f"Lecture du fichier csv : {tmp_file.name}")
                df = pd.read_csv(tmp_file.name, sep=";", dtype={"GEO": "string"})

            finally:
                logger.debug("Suppression du fichier temporaire")
                os.remove(tmp_file.name)

        logger.info("Chargement du csv terminé avec succès")
        return df

    except Exception as e:
        logger.error("Erreur lors du chargement du fichier csv")
        raise Exception(e)