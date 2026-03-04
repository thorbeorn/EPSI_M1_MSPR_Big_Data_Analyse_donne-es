"""Tests unitaires pour [ia]prediction/data_quality.py

Contenu testé:
- upload_json_to_minio
- quality_report

Lancer:
    python -m pytest test_data_quality.py -v
"""

import sys
from pathlib import Path
import pandas as pd
import json
from io import BytesIO
from unittest.mock import Mock, patch, MagicMock
import pytest
from datetime import datetime

# Configuration du chemin pour charger le module
module_path = Path(__file__).resolve().parents[1] / "[ia]prediction" / "data_quality.py"

# Mock des dépendances MinIO avant import
import types as _types

fake_minio_mod = _types.ModuleType('minio')

class _FakeS3Error(Exception):
    pass

def _fake_minio_ctor(*args, **kwargs):
    return Mock()

fake_minio_mod.Minio = _fake_minio_ctor
fake_minio_error = _types.ModuleType('minio.error')
fake_minio_error.S3Error = _FakeS3Error

sys.modules['minio'] = fake_minio_mod
sys.modules['minio.error'] = fake_minio_error

# Charger le module
import importlib.util
spec = importlib.util.spec_from_file_location("data_quality_module", module_path)
data_quality = importlib.util.module_from_spec(spec)
spec.loader.exec_module(data_quality)


class TestUploadJsonToMinio:
    """Tests pour la fonction upload_json_to_minio"""

    def test_upload_dict_with_auto_name(self):
        """Test l'upload d'un dictionnaire avec génération automatique du nom"""
        # Arrange
        test_data = {"key": "value", "number": 42}
        
        with patch.object(data_quality, 'Minio') as mock_minio_class:
            mock_client = Mock()
            mock_minio_class.return_value = mock_client
            mock_client.bucket_exists.return_value = True
            
            # Act
            data_quality.upload_json_to_minio(test_data)
            
            # Assert
            mock_client.put_object.assert_called_once()
            call_kwargs = mock_client.put_object.call_args[1]
            assert call_kwargs['bucket_name'] == "ia-data"
            assert "data_quality_report_" in call_kwargs['object_name']
            assert call_kwargs['content_type'] == "application/json"

    def test_upload_with_custom_name(self):
        """Test l'upload avec un nom personnalisé"""
        # Arrange
        test_data = {"key": "value"}
        custom_name = "custom_report.json"
        
        with patch.object(data_quality, 'Minio') as mock_minio_class:
            mock_client = Mock()
            mock_minio_class.return_value = mock_client
            mock_client.bucket_exists.return_value = True
            
            # Act
            data_quality.upload_json_to_minio(test_data, object_name=custom_name)
            
            # Assert
            mock_client.put_object.assert_called_once()
            call_kwargs = mock_client.put_object.call_args[1]
            assert call_kwargs['object_name'] == custom_name

    def test_upload_with_custom_bucket(self):
        """Test l'upload vers un bucket personnalisé"""
        # Arrange
        test_data = {"data": "test"}
        custom_bucket = "custom-bucket"
        
        with patch.object(data_quality, 'Minio') as mock_minio_class:
            mock_client = Mock()
            mock_minio_class.return_value = mock_client
            mock_client.bucket_exists.return_value = True
            
            # Act
            data_quality.upload_json_to_minio(
                test_data, 
                bucket_name=custom_bucket,
                object_name="test.json"
            )
            
            # Assert
            mock_client.put_object.assert_called_once()
            call_kwargs = mock_client.put_object.call_args[1]
            assert call_kwargs['bucket_name'] == custom_bucket

    def test_bucket_creation(self):
        """Test la création automatique du bucket s'il n'existe pas"""
        # Arrange
        test_data = {"test": "data"}
        
        with patch.object(data_quality, 'Minio') as mock_minio_class:
            mock_client = Mock()
            mock_minio_class.return_value = mock_client
            mock_client.bucket_exists.return_value = False
            
            # Act
            data_quality.upload_json_to_minio(
                test_data, 
                bucket_name="new-bucket",
                object_name="test.json"
            )
            
            # Assert
            mock_client.make_bucket.assert_called_once_with("new-bucket")

    def test_upload_with_special_characters(self):
        """Test l'upload avec caractères spéciaux en JSON"""
        # Arrange
        test_data = {"name": "François", "city": "Québec", "emoji": "🎉"}
        
        with patch.object(data_quality, 'Minio') as mock_minio_class:
            mock_client = Mock()
            mock_minio_class.return_value = mock_client
            mock_client.bucket_exists.return_value = True
            
            # Act
            data_quality.upload_json_to_minio(test_data, object_name="test.json")
            
            # Assert
            mock_client.put_object.assert_called_once()
            call_kwargs = mock_client.put_object.call_args[1]
            # Vérifier que ensure_ascii=False permet les caractères spéciaux
            assert isinstance(call_kwargs['data'], BytesIO)

    def test_connection_error_handling(self):
        """Test la gestion des erreurs de connexion"""
        # Arrange
        test_data = {"test": "data"}
        
        with patch.object(data_quality, 'Minio') as mock_minio_class:
            mock_client = Mock()
            mock_minio_class.return_value = mock_client
            mock_client.bucket_exists.return_value = True
            mock_client.put_object.side_effect = _FakeS3Error("Connection failed")
            
            # Act & Assert - La fonction ne lève pas d'exception mais log l'erreur
            try:
                data_quality.upload_json_to_minio(test_data, object_name="test.json")
            except:
                pytest.fail("upload_json_to_minio ne devrait pas lever d'exception")


class TestQualityReport:
    """Tests pour la fonction quality_report"""

    def test_quality_report_basic(self):
        """Test la génération d'un rapport de qualité basique"""
        # Arrange
        df = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        
        with patch.object(data_quality, 'upload_json_to_minio') as mock_upload:
            # Act
            data_quality.quality_report(df, "test_dataset", "report.json")
            
            # Assert
            mock_upload.assert_called_once()
            call_kwargs = mock_upload.call_args[1]
            report = call_kwargs['data']
            
            assert report["dataset"] == "test_dataset"
            assert report["rows"] == 3
            assert report["columns"] == 2

    def test_quality_report_with_nulls(self):
        """Test le rapport avec valeurs nulles"""
        # Arrange
        df = pd.DataFrame({
            "col1": [1, None, 3],
            "col2": ["a", None, "c"]
        })
        
        with patch.object(data_quality, 'upload_json_to_minio') as mock_upload:
            # Act
            data_quality.quality_report(df, "test_dataset", "report.json")
            
            # Assert
            call_kwargs = mock_upload.call_args[1]
            report = call_kwargs['data']
            
            assert report["null_values"]["col1"] == 1
            assert report["null_values"]["col2"] == 1

    def test_quality_report_with_duplicates(self):
        """Test le rapport avec lignes dupliquées"""
        # Arrange
        df = pd.DataFrame({
            "col1": [1, 1, 2],
            "col2": ["a", "a", "b"]
        })
        
        with patch.object(data_quality, 'upload_json_to_minio') as mock_upload:
            # Act
            data_quality.quality_report(df, "test_dataset", "report.json")
            
            # Assert
            call_kwargs = mock_upload.call_args[1]
            report = call_kwargs['data']
            
            assert report["duplicate_rows"] == 1

    def test_quality_report_dtypes(self):
        """Test que les types de données sont correctement rapportés"""
        # Arrange
        df = pd.DataFrame({
            "int_col": [1, 2, 3],
            "str_col": ["a", "b", "c"],
            "float_col": [1.1, 2.2, 3.3]
        })
        
        with patch.object(data_quality, 'upload_json_to_minio') as mock_upload:
            # Act
            data_quality.quality_report(df, "test_dataset", "report.json")
            
            # Assert
            call_kwargs = mock_upload.call_args[1]
            report = call_kwargs['data']
            
            assert "dtypes" in report
            assert len(report["dtypes"]) == 3

    def test_quality_report_empty_dataframe(self):
        """Test le rapport pour un DataFrame vide"""
        # Arrange
        df = pd.DataFrame()
        
        with patch.object(data_quality, 'upload_json_to_minio') as mock_upload:
            # Act
            data_quality.quality_report(df, "empty_dataset", "report.json")
            
            # Assert
            call_kwargs = mock_upload.call_args[1]
            report = call_kwargs['data']
            
            assert report["rows"] == 0
            assert report["columns"] == 0

    def test_quality_report_large_dataframe(self):
        """Test le rapport pour un grand DataFrame"""
        # Arrange
        df = pd.DataFrame({
            f"col{i}": range(1000) for i in range(10)
        })
        
        with patch.object(data_quality, 'upload_json_to_minio') as mock_upload:
            # Act
            data_quality.quality_report(df, "large_dataset", "report.json")
            
            # Assert
            call_kwargs = mock_upload.call_args[1]
            report = call_kwargs['data']
            
            assert report["rows"] == 1000
            assert report["columns"] == 10

    def test_quality_report_object_name(self):
        """Test que le nom d'objet est correctement transmis"""
        # Arrange
        df = pd.DataFrame({"col": [1, 2, 3]})
        object_name = "custom_report_20260304.json"
        
        with patch.object(data_quality, 'upload_json_to_minio') as mock_upload:
            # Act
            data_quality.quality_report(df, "test_dataset", object_name)
            
            # Assert
            call_kwargs = mock_upload.call_args[1]
            assert call_kwargs['object_name'] == object_name


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

