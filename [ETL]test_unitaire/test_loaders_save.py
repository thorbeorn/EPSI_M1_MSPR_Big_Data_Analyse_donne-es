"""Tests unitaires pour [load]loaders/save.py

Couverture:
- build_indicateurs_table
- save_all_silver_dataframes

Ces tests mockent `pandas.DataFrame.to_sql` pour éviter toute connexion réelle.
"""

import importlib.util
import sys
from pathlib import Path
import pandas as pd


module_path = Path(__file__).resolve().parents[1] / "[load]loaders" / "save.py"
spec = importlib.util.spec_from_file_location("load_save_module", module_path)
save = importlib.util.module_from_spec(spec)
sys.modules["load_save_module"] = save
spec.loader.exec_module(save)


def test_build_indicateurs_table_basic(monkeypatch):
    # Préparer des dataframes silver variés
    df_full = pd.DataFrame({
        "Code_departement": [75, 75, 13],
        "annee": [2020, 2020, 2021]
    })

    df_code_only = pd.DataFrame({"Code_departement": [69, 75]})
    df_annee_only = pd.DataFrame({"annee": [2019, 2020]})

    dfs = {
        "silver_full": df_full,
        "silver_code": df_code_only,
        "silver_annee": df_annee_only,
        "other": 123
    }

    calls = []

    def fake_to_sql(self, name, con, if_exists="replace", index=False, **kwargs):
        calls.append({"name": name, "df": self.copy()})

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    # Appel
    save.build_indicateurs_table(dfs)

    # Doit y avoir un appel vers la table 'indicateurs'
    names = [c["name"] for c in calls]
    assert "indicateurs" in names

    # Vérifier contenu du DataFrame envoyé
    indic_call = next(c for c in calls if c["name"] == "indicateurs")
    indic_df = indic_call["df"]

    # Colonnes attendues
    assert set(indic_df.columns) == {"Code_departement", "annee"}

    # Aucune ligne entièrement nulle
    assert not indic_df["Code_departement"].isna().all()


def test_save_all_silver_dataframes_invokes_to_sql_and_build(monkeypatch):
    df_clients = pd.DataFrame({"a": [1, 2]})
    df_other = pd.DataFrame({"b": [3, 4]})

    dfs = {
        "silver_clients_df": df_clients,
        "silver_other": df_other,
        "not_silver": "ignore"
    }

    to_sql_calls = []

    def fake_to_sql(self, name, con, if_exists="replace", index=False, **kwargs):
        to_sql_calls.append({"name": name, "df": self.copy()})

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    build_called = {}

    def fake_build_indicateurs_table(arg):
        build_called["called_with"] = arg

    monkeypatch.setattr(save, "build_indicateurs_table", fake_build_indicateurs_table)

    save.save_all_silver_dataframes(dfs)

    # Vérifier que chaque DataFrame silver a déclenché un to_sql
    names = [c["name"] for c in to_sql_calls]
    assert "clients" in names
    assert "other" in names

    # Vérifier que build_indicateurs_table a été appelé avec le dictionnaire original
    assert "called_with" in build_called
    assert build_called["called_with"] is dfs
