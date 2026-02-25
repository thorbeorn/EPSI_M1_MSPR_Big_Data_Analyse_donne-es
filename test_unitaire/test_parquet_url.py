"""
Tests unitaires pour creer_dataframe_depuis_parquet_url

Objectifs :
- Isoler l’appel réseau (mock requests.get)
- Vérifier la lecture parquet
- Vérifier l’ajout des métadonnées
- Tester les cas d’erreur

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
module_path = Path(__file__).resolve().parents[1] / "[raw]requesters" / "parquet.py"
spec = importlib.util.spec_from_file_location("parquet_raw_module", module_path)
parquet_raw_module = importlib.util.module_from_spec(spec)
sys.modules["parquet_raw_module"] = parquet_raw_module
spec.loader.exec_module(parquet_raw_module)

creer_dataframe_depuis_parquet_url = parquet_raw_module.creer_dataframe_depuis_parquet_url

# UTILITAIRE : création parquet en mémoire
def create_parquet_bytes():
    """Crée un fichier parquet en mémoire"""
    df = pd.DataFrame({
        "A": [1, 2],
        "B": [3, 4]
    })

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer.read()


def mock_response(content_bytes):
    """Simule une réponse HTTP"""
    mock_resp = Mock()
    mock_resp.content = content_bytes
    mock_resp.raise_for_status = Mock()
    return mock_resp

# TEST 1 : cas nominal avec metadata dict
@patch("parquet_raw_module.requests.get")
def test_parquet_with_dict_metadata(mock_get):
    parquet_bytes = create_parquet_bytes()
    mock_get.return_value = mock_response(parquet_bytes)

    metadata = {"source": "test"}

    df = creer_dataframe_depuis_parquet_url("http://fake/test.parquet", metadata)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.attrs["metadata"]["source"] == "test"

# TEST 2 : metadata depuis fichier JSON
@patch("parquet_raw_module.requests.get")
def test_parquet_with_json_file(mock_get, tmp_path):
    parquet_bytes = create_parquet_bytes()
    mock_get.return_value = mock_response(parquet_bytes)

    # Création fichier JSON temporaire
    metadata_file = tmp_path / "metadata.json"
    metadata_file.write_text('{"type": "json_test"}', encoding="utf-8")

    df = creer_dataframe_depuis_parquet_url(
        "http://fake/test.parquet",
        str(metadata_file)
    )

    assert df.attrs["metadata"]["type"] == "json_test"

# TEST 3 : erreur HTTP
@patch("parquet_raw_module.requests.get")
def test_http_error(mock_get):
    mock_get.side_effect = Exception("Erreur réseau")

    with pytest.raises(Exception):
        creer_dataframe_depuis_parquet_url(
            "http://fake/test.parquet",
            {"test": True}
        )