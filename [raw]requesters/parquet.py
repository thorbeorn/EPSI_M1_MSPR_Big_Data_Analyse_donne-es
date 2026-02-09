import pandas as pd
import json
from typing import Union
import fastparquet
import requests

def creer_dataframe_depuis_parquet(parquet_url: str, temp_file_path: str, metadata_json: Union[str, dict]) -> pd.DataFrame:
    """
    Charge un fichier parquet et applique des métadonnées.
    
    Parameters
    ----------
    parquet_path : str
        URL du fichier parquet
    temp_file_path : str
        PATH du fichier parquet temporaire
    metadata_json : str ou dict
        Chemin vers un fichier JSON de métadonnées ou dictionnaire
    
    Returns
    -------
    pd.DataFrame
    """
    
    # Charger les métadonnées
    if isinstance(metadata_json, str):
        with open(metadata_json, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    else:
        metadata = metadata_json

    r = requests.get(parquet_url, verify=False)
    open(temp_file_path, "wb").write(r.content)

    # Lire le parquet
    df = pd.read_parquet(temp_file_path)

    # Ajouter les métadonnées au DataFrame
    df.attrs["metadata"] = metadata

    return df
