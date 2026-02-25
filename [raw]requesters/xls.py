import pandas as pd
import requests
import tempfile

def creer_dataframe_depuis_xls_url(xls_url: str, sheet_name: str) -> pd.DataFrame:
    """
    Charge un fichier parquet et applique des métadonnées.
    
    Parameters
    ----------
    xls_url : str
        URL du fichier xls
    sheet_name : str
        nom de la feuile à extraire
    
    Returns
    -------
    pd.DataFrame
    """

    r = requests.get(xls_url, verify=False)
    r.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp_file:
        tmp_file.write(r.content)
        tmp_file.flush()
        
        df = pd.read_excel(tmp_file.name, sheet_name=sheet_name)

    return df
