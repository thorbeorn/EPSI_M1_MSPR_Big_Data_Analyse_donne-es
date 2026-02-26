"""
Tests unitaires du module melodi

Objectif :
Tester la fonction creer_dataframe_depuis_melodi_api_url en isolant :
- les appels réseau (mock de requests.get)
- le parsing JSON
- la transformation en DataFrame pandas

Les tests couvrent :
- cas nominal
- absence de valeur dans les mesures
- JSON invalide
- liste d'observations vide

Framework utilisé : pytest
"""

import json
import pandas as pd
from pathlib import Path
from unittest.mock import patch, Mock
import importlib.util
import sys

# Import dynamique du module
module_path = Path(__file__).resolve().parents[1] / "[raw]requesters" / "melodi.py"
spec = importlib.util.spec_from_file_location("melodi_raw_module", module_path)
melodi_raw_module = importlib.util.module_from_spec(spec)
sys.modules["melodi_raw_module"] = melodi_raw_module
spec.loader.exec_module(melodi_raw_module)

# Ajoute la fonction dans un variable pour l'instancié plus tard
creer_dataframe = melodi_raw_module.creer_dataframe_depuis_melodi_api_url

# -------------------------------------------------------------------
# DONNÉES SIMULÉES (FAKE RESPONSE)
# -------------------------------------------------------------------
# Cette fonction simule la réponse de l'API MELODI.
# Elle retourne un objet Mock contenant un JSON valide.
def fake_response():
    data = {
        "title": {"fr": "Test dataset"},
        "identifier": "TEST",
        "observations": [
            {
                "dimensions": {"ANNEE": "2024"},
                "attributes": {"SOURCE": "UNIT_TEST"},
                "measures": {
                    "OBS_VALUE_NIVEAU": {
                        "value": 100
                    }
                }
            }
        ]
    }

    mock_resp = Mock()
    mock_resp.content = json.dumps(data).encode("utf-8")
    return mock_resp

# -------------------------------------------------------------------
# TEST 1 : CAS NOMINAL
# -------------------------------------------------------------------
# Vérifie que :
# - la requête HTTP est simulée
# - un DataFrame est bien retourné
# - les données sont correctement extraites et transformées
@patch("melodi_raw_module.requests.get")
def test_creer_dataframe_depuis_melodi_api_url(mock_get):
    # Arrange
    mock_get.return_value = fake_response()

    # Act
    df = creer_dataframe("http://fake-url")

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["ANNEE"] == "2024"
    assert df.iloc[0]["SOURCE"] == "UNIT_TEST"
    assert df.iloc[0]["OBS_VALUE_NIVEAU"] == 100

# -------------------------------------------------------------------
# TEST 2 : OBS_VALUE_NIVEAU SANS "value"
# -------------------------------------------------------------------
# Vérifie le comportement lorsque la mesure existe mais ne contient
# pas de clé "value". La fonction doit retourner None.
@patch("melodi_raw_module.requests.get")
def test_sans_value(mock_get):
    data = {
        "title": {"fr": "Test"},
        "identifier": "TEST",
        "observations": [
            {
                "dimensions": {"ANNEE": "2024"},
                "attributes": {"SOURCE": "TEST"},
                "measures": {
                    "OBS_VALUE_NIVEAU": {}
                }
            }
        ]
    }

    mock_resp = Mock()
    mock_resp.content = json.dumps(data).encode("utf-8")
    mock_get.return_value = mock_resp

    df = creer_dataframe("fake-url")

    # La valeur doit être None
    assert df.iloc[0]["OBS_VALUE_NIVEAU"] is None

# -------------------------------------------------------------------
# TEST 3 : JSON INVALIDE
# -------------------------------------------------------------------
# Vérifie que la fonction lève une exception lorsque la réponse API
# ne contient pas un JSON valide.
@patch("melodi_raw_module.requests.get")
def test_json_invalide(mock_get):
    mock_resp = Mock()
    mock_resp.content = b"not a json"
    mock_get.return_value = mock_resp

    import pytest
    with pytest.raises(Exception):
        creer_dataframe("fake-url")

# -------------------------------------------------------------------
# TEST 4 : OBSERVATIONS VIDES
# -------------------------------------------------------------------
# Vérifie que la fonction retourne un DataFrame vide lorsque
# aucune observation n'est présente dans la réponse.
@patch("melodi_raw_module.requests.get")
def test_observations_vides(mock_get):
    data = {
        "title": {"fr": "Test"},
        "identifier": "TEST",
        "observations": []
    }

    mock_resp = Mock()
    mock_resp.content = json.dumps(data).encode("utf-8")
    mock_get.return_value = mock_resp

    df = creer_dataframe("fake-url")

    assert df.empty