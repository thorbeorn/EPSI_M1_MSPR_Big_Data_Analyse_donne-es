import pandas as pd
import json
from typing import Union
import fastparquet
import requests
import tempfile

def creer_dataframe_depuis_parquet_url(parquet_url: str, metadata_json: Union[str, dict]) -> pd.DataFrame:
    """
    Charge un fichier parquet et applique des métadonnées.
    
    Parameters
    ----------
    parquet_path : str
        URL du fichier parquet
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
    r.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_file:
        tmp_file.write(r.content)
        tmp_file.flush()
        
        df = pd.read_parquet(tmp_file.name)

    # Ajouter les métadonnées au DataFrame
    df.attrs["metadata"] = metadata

    return df
