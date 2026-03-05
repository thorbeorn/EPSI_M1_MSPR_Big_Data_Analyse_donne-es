"""
Tests unitaires pour creer_dataframe_depuis_csv_url

Objectifs :
- Isoler l'appel réseau (mock requests.get)
- Vérifier la lecture d'un fichier CSV simple
- Vérifier la lecture d'un fichier ZIP contenant un CSV
- Tester les erreurs HTTP
- Tester les fichiers ZIP sans le CSV attendu

Framework : pytest
"""

import pandas as pd
import io
import zipfile
from unittest.mock import patch, Mock
from pathlib import Path
import importlib.util
import sys
import pytest


# IMPORT DYNAMIQUE (compatible dossier [raw]requesters)
module_path = Path(__file__).resolve().parents[1] / "[raw]requesters" / "csv.py"
spec = importlib.util.spec_from_file_location("csv_raw_module", module_path)
csv_raw_module = importlib.util.module_from_spec(spec)
sys.modules["csv_raw_module"] = csv_raw_module
spec.loader.exec_module(csv_raw_module)

creer_dataframe_depuis_csv_url = csv_raw_module.creer_dataframe_depuis_csv_url

# UTILITAIRE : création CSV en mémoire
def create_csv_bytes():
    """Crée un fichier CSV en mémoire"""
    csv_content = "GEO;valeur\nFR-75;100\nFR-92;200\n"
    return csv_content.encode('utf-8')

# UTILITAIRE : création ZIP en mémoire avec CSV
def create_zip_bytes():
    """Crée un fichier ZIP en mémoire contenant un CSV spécifique"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        csv_content = "GEO;valeur\nFR-75;100\nFR-92;200\n"
        zip_file.writestr("DS_BPE_SPORT_CULTURE_2024_data.csv", csv_content)
    buffer.seek(0)
    return buffer.read()

def mock_response(content_bytes):
    """Simule une réponse HTTP"""
    mock_resp = Mock()
    mock_resp.content = content_bytes
    mock_resp.raise_for_status = Mock()
    return mock_resp

# TEST 1 : cas nominal CSV simple
@patch("csv_raw_module.requests.get")
def test_csv_nominal(mock_get):
    csv_bytes = create_csv_bytes()
    mock_get.return_value = mock_response(csv_bytes)

    df = creer_dataframe_depuis_csv_url("http://fake/test.csv")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "GEO" in df.columns
    assert df["GEO"].iloc[0] == "FR-75"

# TEST 2 : cas nominal ZIP
@patch("csv_raw_module.requests.get")
def test_zip_nominal(mock_get):
    zip_bytes = create_zip_bytes()
    mock_get.return_value = mock_response(zip_bytes)

    df = creer_dataframe_depuis_csv_url("http://fake/test.zip")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "GEO" in df.columns
    assert df["GEO"].iloc[0] == "FR-75"

# TEST 3 : ZIP sans le fichier attendu
@patch("csv_raw_module.requests.get")
def test_zip_missing_file(mock_get):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        csv_content = "GEO;valeur\nFR-75;100\n"
        zip_file.writestr("wrong_name.csv", csv_content)  # Mauvais nom
    buffer.seek(0)
    zip_bytes = buffer.read()
    mock_get.return_value = mock_response(zip_bytes)

    with pytest.raises(Exception, match="DS_BPE_SPORT_CULTURE_2024_data.csv introuvable"):
        creer_dataframe_depuis_csv_url("http://fake/test.zip")

# TEST 4 : erreur HTTP
@patch("csv_raw_module.requests.get")
def test_http_error(mock_get):
    mock_get.side_effect = Exception("Erreur réseau")

    with pytest.raises(Exception):
        creer_dataframe_depuis_csv_url("http://fake/test.csv")

# TEST 5 : CSV malformé
@patch("csv_raw_module.requests.get")
def test_csv_malformed(mock_get):
    malformed_csv = "not;csv\ncontent\n"
    mock_get.return_value = mock_response(malformed_csv.encode('utf-8'))

    # Ne devrait pas lever d'exception, pandas gère les erreurs
    df = creer_dataframe_depuis_csv_url("http://fake/test.csv")
    assert isinstance(df, pd.DataFrame)