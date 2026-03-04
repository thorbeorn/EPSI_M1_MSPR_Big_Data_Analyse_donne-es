"""Tests unitaires pour [ia]prediction/load.py

Contenu testé:
- load_parquet_from_minio

Lancer:
    python -m pytest test_load.py -v
"""

import sys
from pathlib import Path
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch, MagicMock
import pytest

# Configuration du chemin pour charger le module
module_path = Path(__file__).resolve().parents[1] / "[ia]prediction" / "load.py"

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
spec = importlib.util.spec_from_file_location("load_module", module_path)
load = importlib.util.module_from_spec(spec)
spec.loader.exec_module(load)


class TestLoadParquetFromMinio:
    """Tests pour la fonction load_parquet_from_minio"""

    def _create_parquet_bytes(self, df):
        """Création d'un fichier parquet en bytes pour le mock"""
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        return buffer.getvalue()

    def test_load_parquet_basic(self):
        """Test le chargement basique d'un fichier parquet"""
        # Arrange
        df = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        parquet_bytes = self._create_parquet_bytes(df)
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = parquet_bytes
            mock_client.get_object.return_value = mock_response
            
            object_name = "test_file.parquet"
            
            # Act
            result_df = load.load_parquet_from_minio(object_name)
            
            # Assert
            assert isinstance(result_df, pd.DataFrame)
            assert len(result_df) == 3
            assert list(result_df.columns) == ["col1", "col2"]
            mock_client.get_object.assert_called_once_with("gold", object_name)

    def test_load_parquet_data_integrity(self):
        """Test que les données sont correctement conservées lors du chargement"""
        # Arrange
        df = pd.DataFrame({
            "int_col": [1, 2, 3],
            "str_col": ["x", "y", "z"],
            "float_col": [1.1, 2.2, 3.3]
        })
        parquet_bytes = self._create_parquet_bytes(df)
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = parquet_bytes
            mock_client.get_object.return_value = mock_response
            
            # Act
            result_df = load.load_parquet_from_minio("test.parquet")
            
            # Assert
            pd.testing.assert_frame_equal(result_df, df)

    def test_load_parquet_large_file(self):
        """Test le chargement d'un fichier parquet volumineux"""
        # Arrange
        df = pd.DataFrame({
            f"col{i}": range(10000) for i in range(5)
        })
        parquet_bytes = self._create_parquet_bytes(df)
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = parquet_bytes
            mock_client.get_object.return_value = mock_response
            
            # Act
            result_df = load.load_parquet_from_minio("large_file.parquet")
            
            # Assert
            assert len(result_df) == 10000
            assert len(result_df.columns) == 5

    def test_load_parquet_with_null_values(self):
        """Test le chargement avec valeurs nulles"""
        # Arrange
        df = pd.DataFrame({
            "col1": [1, None, 3],
            "col2": ["a", None, "c"]
        })
        parquet_bytes = self._create_parquet_bytes(df)
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = parquet_bytes
            mock_client.get_object.return_value = mock_response
            
            # Act
            result_df = load.load_parquet_from_minio("null_values.parquet")
            
            # Assert
            assert result_df.isnull().sum().sum() == 2
            assert result_df.iloc[1, 0] is pd.NA or pd.isna(result_df.iloc[1, 0])

    def test_load_parquet_s3_error(self):
        """Test la gestion des erreurs S3/MinIO"""
        # Arrange
        with patch.object(load, 'client') as mock_client:
            mock_client.get_object.side_effect = _FakeS3Error("Bucket not found")
            
            # Act & Assert
            with pytest.raises(_FakeS3Error):
                load.load_parquet_from_minio("nonexistent.parquet")

    def test_load_parquet_corrupted_file(self):
        """Test la gestion des fichiers corrompus"""
        # Arrange
        corrupted_data = b"This is not a valid parquet file"
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = corrupted_data
            mock_client.get_object.return_value = mock_response
            
            # Act & Assert
            with pytest.raises(Exception):  # Parquet reading will fail
                load.load_parquet_from_minio("corrupted.parquet")

    def test_load_parquet_closes_connection(self):
        """Test que la connexion est correctement fermée"""
        # Arrange
        df = pd.DataFrame({"col": [1, 2, 3]})
        parquet_bytes = self._create_parquet_bytes(df)
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = parquet_bytes
            mock_response.close = Mock()
            mock_response.release_conn = Mock()
            mock_client.get_object.return_value = mock_response
            
            # Act
            load.load_parquet_from_minio("test.parquet")
            
            # Assert
            mock_response.close.assert_called_once()
            mock_response.release_conn.assert_called_once()

    def test_load_parquet_bucket_name(self):
        """Test que le bucket par défaut 'gold' est utilisé"""
        # Arrange
        df = pd.DataFrame({"col": [1, 2, 3]})
        parquet_bytes = self._create_parquet_bytes(df)
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = parquet_bytes
            mock_client.get_object.return_value = mock_response
            
            object_name = "test_file.parquet"
            
            # Act
            load.load_parquet_from_minio(object_name)
            
            # Assert
            mock_client.get_object.assert_called_once_with("gold", object_name)

    def test_load_parquet_empty_dataframe(self):
        """Test le chargement d'un DataFrame vide"""
        # Arrange
        df = pd.DataFrame()
        parquet_bytes = self._create_parquet_bytes(df)
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = parquet_bytes
            mock_client.get_object.return_value = mock_response
            
            # Act
            result_df = load.load_parquet_from_minio("empty.parquet")
            
            # Assert
            assert len(result_df) == 0

    def test_load_parquet_with_special_types(self):
        """Test le chargement avec types de données spéciaux"""
        # Arrange
        df = pd.DataFrame({
            "date_col": pd.date_range("2020-01-01", periods=3),
            "bool_col": [True, False, True],
            "int64_col": [1, 2, 3]
        })
        parquet_bytes = self._create_parquet_bytes(df)
        
        with patch.object(load, 'client') as mock_client:
            mock_response = Mock()
            mock_response.read.return_value = parquet_bytes
            mock_client.get_object.return_value = mock_response
            
            # Act
            result_df = load.load_parquet_from_minio("special_types.parquet")
            
            # Assert
            assert pd.api.types.is_datetime64_any_dtype(result_df["date_col"])
            assert pd.api.types.is_bool_dtype(result_df["bool_col"])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

