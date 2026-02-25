import os
import requests
import zipfile
import tempfile
import pandas as pd

def creer_dataframe_depuis_multiple_url(multiple_path: dict) -> dict:
    """
    Charge plusieurs fichiers Excel depuis des URLs (xlsx ou zip).
    Chaque feuille devient un DataFrame séparé.

    Returns
    -------
    dict
        {
            "annee": {
                "nom_feuille": DataFrame
            }
        }
    """

    dataframes = {}
    temp_dir = tempfile.mkdtemp()

    for year, url in multiple_path.items():
        dataframes[year] = {}

        # Téléchargement
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        filename = os.path.join(temp_dir, url.split("/")[-1])
        
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # --- Cas 1 : fichier Excel direct ---
        if filename.endswith(".xlsx"):
            sheets = pd.read_excel(filename, sheet_name=None)
            for sheet_name, df in sheets.items():
                dataframes[year][sheet_name] = df

        # --- Cas 2 : ZIP contenant des Excel ---
        elif filename.endswith(".zip"):
            extract_path = os.path.join(temp_dir, year)
            os.makedirs(extract_path, exist_ok=True)
            
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(extract_path)

            # Parcourir les Excel extraits
            for root, _, files in os.walk(extract_path):
                for file in files:
                    if file.endswith(".xlsx"):
                        file_path = os.path.join(root, file)
                        
                        sheets = pd.read_excel(file_path, sheet_name=None)
                        for sheet_name, df in sheets.items():
                            # Si même nom de feuille dans plusieurs fichiers → concat
                            if sheet_name in dataframes[year]:
                                dataframes[year][sheet_name] = pd.concat(
                                    [dataframes[year][sheet_name], df],
                                    ignore_index=True
                                )
                            else:
                                dataframes[year][sheet_name] = df

    return dataframes