import pandas as pd
import json
from typing import Union
import os
import requests
import tempfile
import logging
import urllib3

# Set le logger pour les logs
logger = logging.getLogger(__name__)
# Desactivation du warning externe pour le certificat
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
Télécharge un fichier Parquet depuis une URL et y associe des métadonnées.

Cette fonction appartient à la couche RAW de l’ETL :
- Téléchargement du fichier distant
- Lecture brute en DataFrame
- Ajout de métadonnées (sans transformation métier)

Parameters
----------
parquet_url : str
    URL du fichier parquet à télécharger
metadata_json : str ou dict
    Chemin vers un fichier JSON de métadonnées
    ou dictionnaire contenant les métadonnées

Returns
-------
pd.DataFrame
    DataFrame contenant les données parquet avec
    les métadonnées stockées dans df.attrs["metadata"]
"""
def creer_dataframe_depuis_parquet_url(parquet_url: str, metadata_json: Union[str, dict]) -> pd.DataFrame:
    logger.info("Début du chargement du fichier parquet depuis URL")

    try:
        # Chargement des métadonnées
        logger.debug("Chargement des métadonnées")

        if isinstance(metadata_json, str):
            logger.debug(f"Lecture du fichier metadata : {metadata_json}")
            with open(metadata_json, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        else:
            logger.debug("Métadonnées fournies sous forme de dictionnaire")
            metadata = metadata_json

        # Téléchargement du fichier parquet
        logger.debug(f"Téléchargement du fichier parquet depuis : {parquet_url}")

        response = requests.get(parquet_url, verify=False)
        response.raise_for_status()

        # Sauvegarde temporaire du fichier (nécessaire pour Windows)
        logger.debug("Création d'un fichier temporaire pour lecture parquet")

        tmp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)

        try:
            tmp_file.write(response.content)
            tmp_file.close()  # Obligatoire avant lecture sous Windows

            logger.debug(f"Lecture du fichier parquet : {tmp_file.name}")
            df = pd.read_parquet(tmp_file.name)

        finally:
            # Nettoyage du fichier temporaire
            logger.debug("Suppression du fichier temporaire")
            os.remove(tmp_file.name)

        # Ajout des métadonnées au DataFrame
        logger.debug("Ajout des métadonnées au DataFrame")
        df.attrs["metadata"] = metadata

        logger.info("Chargement du parquet terminé avec succès")
        return df

    except Exception as e:
        logger.error("Erreur lors du chargement du fichier parquet")
        raise Exception(e)
