import pandas as pd
import requests

def creer_dataframe_depuis_xls_url(xls_url: str, temp_file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Charge un fichier parquet et applique des métadonnées.
    
    Parameters
    ----------
    xls_url : str
        URL du fichier xls
    temp_file_path : str
        PATH du fichier xls temporaire
    
    Returns
    -------
    pd.DataFrame
    """

    # Telecharger le xls
    r = requests.get(xls_url, verify=False)
    open(temp_file_path, "wb").write(r.content)

    # Lire le xls
    df = pd.read_excel(temp_file_path, sheet_name)

    return df
