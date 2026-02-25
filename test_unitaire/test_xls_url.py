"""
Tests unitaires pour creer_dataframe_depuis_xls_url

Objectifs :
- Isoler l'appel réseau (mock requests.get)
- Vérifier la lecture d'une feuille Excel
- Tester les erreurs HTTP
- Tester les feuilles inexistantes

Framework : pytest
"""

import pandas as pd
import io
from unittest.mock import patch, Mock
from pathlib import Path
import importlib.util
import sys
import pytest


# IMPORT DYNAMIQUE (compatible dossier [raw]requesters)
module_path = Path(__file__).resolve().parents[1] / "[raw]requesters" / "xls.py"
spec = importlib.util.spec_from_file_location("xls_raw_module", module_path)
xls_raw_module = importlib.util.module_from_spec(spec)
sys.modules["xls_raw_module"] = xls_raw_module
spec.loader.exec_module(xls_raw_module)

creer_dataframe_depuis_xls_url = xls_raw_module.creer_dataframe_depuis_xls_url

# UTILITAIRE : création Excel en mémoire
def create_excel_bytes():
    """Crée un fichier Excel en mémoire avec deux feuilles"""
    buffer = io.BytesIO()

    df1 = pd.DataFrame({"A": [1, 2]})
    df2 = pd.DataFrame({"B": [3, 4]})

    with pd.ExcelWriter(buffer) as writer:
        df1.to_excel(writer, sheet_name="Sheet1", index=False)
        df2.to_excel(writer, sheet_name="Sheet2", index=False)

    buffer.seek(0)
    return buffer.read()


def mock_response(content_bytes):
    """Simule une réponse HTTP"""
    mock_resp = Mock()
    mock_resp.content = content_bytes
    mock_resp.raise_for_status = Mock()
    return mock_resp

# TEST 1 : cas nominal
@patch("xls_raw_module.requests.get")
def test_xls_nominal(mock_get):
    excel_bytes = create_excel_bytes()
    mock_get.return_value = mock_response(excel_bytes)

    df = creer_dataframe_depuis_xls_url(
        "http://fake/test.xlsx",
        "Sheet1"
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "A" in df.columns

# TEST 2 : feuille inexistante
@patch("xls_raw_module.requests.get")
def test_sheet_not_found(mock_get):
    excel_bytes = create_excel_bytes()
    mock_get.return_value = mock_response(excel_bytes)

    with pytest.raises(Exception):
        creer_dataframe_depuis_xls_url(
            "http://fake/test.xlsx",
            "UnknownSheet"
        )

# TEST 3 : erreur HTTP
@patch("xls_raw_module.requests.get")
def test_http_error(mock_get):
    mock_get.side_effect = Exception("Erreur réseau")

    with pytest.raises(Exception):
        creer_dataframe_depuis_xls_url(
            "http://fake/test.xlsx",
            "Sheet1"
        )