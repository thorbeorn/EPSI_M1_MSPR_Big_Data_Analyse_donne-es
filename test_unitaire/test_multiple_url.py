import pandas as pd
import io
import zipfile
from unittest.mock import patch, Mock
from pathlib import Path
import importlib.util
import sys

# Import dynamique du module RAW
module_path = Path(__file__).resolve().parents[1] / "[raw]requesters" / "mixedxlsxzip.py"
spec = importlib.util.spec_from_file_location("multi_raw_module", module_path)
multi_raw_module = importlib.util.module_from_spec(spec)
sys.modules["multi_raw_module"] = multi_raw_module
spec.loader.exec_module(multi_raw_module)

creer_dataframe_depuis_multiple_url = multi_raw_module.creer_dataframe_depuis_multiple_url


# UTILITAIRES DE GÉNÉRATION DE FICHIERS EN MÉMOIRE
def create_excel_bytes():
    """Crée un fichier Excel en mémoire"""
    buffer = io.BytesIO()
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    buffer.seek(0)
    return buffer.read()


def create_zip_bytes():
    """Crée un ZIP contenant un Excel"""
    excel_bytes = create_excel_bytes()
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("file.xlsx", excel_bytes)

    zip_buffer.seek(0)
    return zip_buffer.read()


def mock_response(content_bytes):
    """Simule une réponse requests"""
    mock_resp = Mock()
    mock_resp.iter_content = lambda chunk_size: [content_bytes]
    mock_resp.raise_for_status = Mock()
    return mock_resp

# TEST 1 : Excel direct
@patch("multi_raw_module.requests.get")
def test_excel_direct(mock_get):
    excel_bytes = create_excel_bytes()
    mock_get.return_value = mock_response(excel_bytes)

    urls = {"2024": "http://fake/test.xlsx"}

    result = creer_dataframe_depuis_multiple_url(urls)

    assert "2024" in result
    assert "Sheet1" in result["2024"]
    assert isinstance(result["2024"]["Sheet1"], pd.DataFrame)
    assert len(result["2024"]["Sheet1"]) == 2


# TEST 2 : ZIP contenant Excel
@patch("multi_raw_module.requests.get")
def test_zip_file(mock_get):
    zip_bytes = create_zip_bytes()
    mock_get.return_value = mock_response(zip_bytes)

    urls = {"2024": "http://fake/test.zip"}

    result = creer_dataframe_depuis_multiple_url(urls)

    assert "2024" in result
    assert "Sheet1" in result["2024"]
    assert len(result["2024"]["Sheet1"]) == 2


# TEST 3 : Erreur HTTP
@patch("multi_raw_module.requests.get")
def test_http_error(mock_get):
    mock_get.side_effect = Exception("Erreur réseau")

    import pytest

    with pytest.raises(Exception):
        creer_dataframe_depuis_multiple_url({"2024": "http://fake/test.xlsx"})