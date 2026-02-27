"""Tests unitaires pour [load]loaders/quality.py

Contenu testé:
- audit_dataframe
- audit_all_silver_dataframes (upload mockée)
- upload_json_to_minio (Minio mocké)

Lancer:
    python -m pytest test_loaders_quality.py -q
"""

import importlib.util
import sys
from pathlib import Path
import pandas as pd
import json
from io import BytesIO


module_path = Path(__file__).resolve().parents[1] / "[load]loaders" / "quality.py"
spec = importlib.util.spec_from_file_location("load_quality_module", module_path)
quality = importlib.util.module_from_spec(spec)
sys.modules["load_quality_module"] = quality
# Inject a fake `minio` module to avoid requiring the real dependency at import time
import types
import types as _types

# Create a proper module object for `minio` and a submodule `minio.error`
fake_minio_mod = _types.ModuleType('minio')

class _FakeS3Error(Exception):
    pass

def _fake_minio_ctor(*args, **kwargs):
    return None

fake_minio_mod.Minio = _fake_minio_ctor
fake_minio_error = _types.ModuleType('minio.error')
fake_minio_error.S3Error = _FakeS3Error

sys.modules['minio'] = fake_minio_mod
sys.modules['minio.error'] = fake_minio_error

spec.loader.exec_module(quality)


def test_audit_dataframe_basic():
    df = pd.DataFrame({
        "num": [1, -2, None],
        "cat": ["x", "x", None]
    })

    report = quality.audit_dataframe(df, "test_df")

    assert report["dataframe_name"] == "test_df"
    assert report["nb_rows"] == 3
    assert report["nb_columns"] == 2
    assert report["duplicates"] == 0

    # Vérifications colonnes
    assert "num" in report["columns"]
    assert "cat" in report["columns"]

    num_report = report["columns"]["num"]
    assert num_report["missing_values"] == 1
    assert "negative_values" in num_report and num_report["negative_values"] == 1

    cat_report = report["columns"]["cat"]
    assert cat_report["missing_values"] == 1

    # Score qualité attendu approx: missing 33.33% par colonne -> avg_missing ~33.33
    assert pytest_approx(report["quality_score"], 66.67)


def pytest_approx(actual, expected, tol=0.05):
    """Petit utilitaire d'approximation pour éviter dépendance à pytest.approx."""
    return abs(actual - expected) <= tol


def test_audit_all_silver_dataframes_calls_upload(monkeypatch):
    df1 = pd.DataFrame({"a": [1, 2, None]})
    df2 = pd.DataFrame({"b": ["x", None, "y"]})

    namespace = {
        "silver_df1": df1,
        "silver_df2": df2,
        "other": 123
    }

    captured = {}

    def fake_upload_json_to_minio(data, bucket_name="data-quality", object_name=None, **kwargs):
        captured['data'] = data
        captured['bucket'] = bucket_name
        captured['object_name'] = object_name

    monkeypatch.setattr(quality, "upload_json_to_minio", fake_upload_json_to_minio)

    reports = quality.audit_all_silver_dataframes(namespace)

    # Retour attendu : deux rapports
    assert isinstance(reports, list)
    assert len(reports) == 2

    # upload_json_to_minio doit avoir été appelé avec les rapports
    assert 'data' in captured
    assert isinstance(captured['data'], list)
    assert len(captured['data']) == 2


def test_upload_json_to_minio_with_mock_minio(monkeypatch):
    # Préparer un fake Minio client
    calls = {}

    class FakeMinio:
        def __init__(self, endpoint, access_key=None, secret_key=None, secure=False):
            calls['init'] = dict(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

        def bucket_exists(self, bucket_name):
            calls.setdefault('bucket_exists', []).append(bucket_name)
            return False

        def make_bucket(self, bucket_name):
            calls.setdefault('make_bucket', []).append(bucket_name)

        def put_object(self, bucket_name, object_name, data, length, content_type=None):
            # lire le contenu du BytesIO
            if hasattr(data, 'read'):
                data.seek(0)
                content = data.read()
            else:
                content = None
            calls.setdefault('put_object', []).append({
                'bucket': bucket_name,
                'object_name': object_name,
                'length': length,
                'content_type': content_type,
                'content': content
            })

    # Remplacer la référence `Minio` dans le module `quality` par notre Fake
    monkeypatch.setattr(quality, 'Minio', FakeMinio)

    data = {"k": "v"}
    # Appel de la fonction ; forcer object_name pour test prévisible
    quality.upload_json_to_minio(data, bucket_name="test-bucket", object_name="test.json")

    assert 'init' in calls
    assert 'make_bucket' in calls
    assert 'put_object' in calls
