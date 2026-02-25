import pandas as pd
import requests
import tempfile
import os
import logging
import urllib3

# Set le logger pour les logs
logger = logging.getLogger(__name__)
# Desactivation du warning externe pour le certificat
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
Télécharge un fichier Excel depuis une URL et retourne une feuille spécifique
sous forme de DataFrame.

Cette fonction appartient à la couche RAW de l’ETL :
- Téléchargement du fichier distant
- Lecture brute d’une feuille Excel
- Aucune transformation métier

Parameters
----------
xls_url : str
    URL du fichier Excel (.xls ou .xlsx)
sheet_name : str
    Nom de la feuille à extraire

Returns
-------
pd.DataFrame
    DataFrame contenant les données de la feuille demandée
"""
def creer_dataframe_depuis_xls_url(xls_url: str, sheet_name: str) -> pd.DataFrame:

    logger.info("Début du chargement du fichier Excel depuis URL")

    try:
        # Téléchargement du fichier
        logger.debug(f"Téléchargement depuis : {xls_url}")

        response = requests.get(xls_url, verify=False)
        response.raise_for_status()

        # Création d'un fichier temporaire (nécessaire sous Windows)
        logger.debug("Création d'un fichier temporaire pour lecture Excel")

        tmp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

        try:
            # Écriture du contenu téléchargé
            tmp_file.write(response.content)
            tmp_file.close()

            logger.debug(f"Lecture de la feuille '{sheet_name}'")

            # Lecture de la feuille demandée
            df = pd.read_excel(tmp_file.name, sheet_name=sheet_name)

        finally:
            # Nettoyage du fichier temporaire
            logger.debug("Suppression du fichier temporaire")
            os.remove(tmp_file.name)

        logger.info("Chargement du fichier Excel terminé avec succès")
        return df

    except Exception as e:
        logger.error("Erreur lors du chargement du fichier Excel")
        raise Exception(e)
