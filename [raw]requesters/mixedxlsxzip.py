import os
import requests
import zipfile
import tempfile
import pandas as pd
import logging
import shutil
import urllib3

# Set le logger pour les logs
logger = logging.getLogger(__name__)
# Desactivation du warning externe pour le certificat
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


"""
Télécharge plusieurs fichiers (Excel ou ZIP) depuis des URLs
et retourne les données sous forme de dictionnaire de DataFrames.

Cette fonction appartient à la couche RAW de l'ETL :
- Téléchargement des fichiers distants
- Extraction si nécessaire
- Lecture des fichiers Excel
- Aucune transformation métier n'est réalisée

Parameters
----------
multiple_path : dict
    Dictionnaire contenant les URLs à télécharger,
    sous la forme :
    {
        "2022": "https://.../fichier.xlsx",
        "2023": "https://.../fichier.zip"
    }

Returns
-------
dict
    Structure retournée :
    {
        "année": {
            "nom_feuille": DataFrame
        }
    }
"""
def creer_dataframe_depuis_multiple_url(multiple_path: dict) -> dict:
    logger.info("Début du téléchargement des données depuis plusieurs URLs")

    try:
        # Initialisation
        logger.debug("Création du dictionnaire de sortie")
        dataframes = {}

        logger.debug("Création d'un dossier temporaire pour stocker les fichiers téléchargés")
        temp_dir = tempfile.mkdtemp()
        logger.debug(f"Dossier temporaire créé : {temp_dir}")

        # Boucle sur chaque URL fournie
        for year, url in multiple_path.items():
            logger.debug(f"Téléchargement depuis l'URL : {url}")

            dataframes[year] = {}

            # Téléchargement du fichier
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Erreur lors du téléchargement pour l'année {year} : {e}")
                raise

            # Nom du fichier local
            filename = os.path.join(temp_dir, url.split("/")[-1])
            logger.debug(f"Enregistrement du fichier dans : {filename}")

            # Écriture du fichier en mode streaming (pour éviter la surcharge mémoire)
            with open(filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Cas 1 : fichier Excel direct (.xlsx)
            if filename.endswith(".xlsx"):
                logger.debug(f"Fichier Excel détecté pour {year}")

                sheets = pd.read_excel(filename, sheet_name=None)
                logger.debug(f"{len(sheets)} feuille(s) trouvée(s)")

                for sheet_name, df in sheets.items():
                    logger.debug(f"Chargement de la feuille : {sheet_name}")
                    dataframes[year][sheet_name] = df

            # Cas 2 : fichier ZIP contenant des Excel
            elif filename.endswith(".zip"):
                logger.debug(f"Fichier ZIP détecté pour {year}")

                extract_path = os.path.join(temp_dir, year)
                os.makedirs(extract_path, exist_ok=True)
                logger.debug(f"Extraction du ZIP dans : {extract_path}")

                with zipfile.ZipFile(filename, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)

                # Parcours récursif des fichiers extraits
                for root, _, files in os.walk(extract_path):
                    for file in files:
                        if file.endswith(".xlsx"):
                            file_path = os.path.join(root, file)
                            logger.debug(f"Lecture du fichier Excel extrait : {file_path}")

                            sheets = pd.read_excel(file_path, sheet_name=None)

                            for sheet_name, df in sheets.items():
                                # Si une feuille du même nom existe déjà,
                                # concaténation des données
                                if sheet_name in dataframes[year]:
                                    logger.debug(
                                        f"Concaténation des données pour la feuille {sheet_name}"
                                    )
                                    dataframes[year][sheet_name] = pd.concat(
                                        [dataframes[year][sheet_name], df],
                                        ignore_index=True
                                    )
                                else:
                                    dataframes[year][sheet_name] = df

            else:
                logger.warning(f"Format de fichier non supporté : {filename}")

        logger.info("Téléchargement et lecture terminés avec succès")

        return dataframes

    except Exception as e:
        logger.error("Une erreur est survenue lors du traitement des URLs multiples")
        raise Exception(e)

    finally:
        # Nettoyage du dossier temporaire
        try:
            shutil.rmtree(temp_dir)
            logger.debug(f"Dossier temporaire supprimé : {temp_dir}")
        except Exception:
            logger.warning("Impossible de supprimer le dossier temporaire")