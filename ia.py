import json
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd

# CONFIGURATION DU LOGGING
LOG_LEVEL = logging.DEBUG

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("ETL_PIPELINE")


def _get_exports_dir():
    """Retourne le dossier d'exports [ia]exports (créé si nécessaire)."""
    exports_dir = os.path.join(os.path.dirname(__file__), "[ia]exports")
    os.makedirs(exports_dir, exist_ok=True)
    return exports_dir


def _to_json_serializable(obj):
    """Recursively convert numpy / pandas types into native Python types."""
    if isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, (np.ndarray, list, tuple)):
        return [_to_json_serializable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _to_json_serializable(v) for k, v in obj.items()}
    return obj


def _write_json(filename: str, data):
    path = os.path.join(_get_exports_dir(), filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_to_json_serializable(data), f, ensure_ascii=False, indent=2)


def _build_model_json(model_name: str, accuracy: float, mae: float, r2: float,
                      classification_report: str, results_2024: dict,
                      extra: dict | None = None) -> dict:
    """Structure standardisée pour tous les modèles."""
    payload = {
        "model": model_name,
        "generated_at": datetime.now().isoformat(),
        "metrics": {
            "accuracy": accuracy,
            "mae": mae,
            "r2": r2,
        },
        "classification_report": classification_report,
        "results_2024": results_2024,
    }
    if extra:
        payload.update(extra)
    return payload


try:
    load_module = getattr(__import__("[ia]prediction.load"), "load")
    data_quality_module = getattr(__import__("[ia]prediction.data_quality"), "data_quality")
except ModuleNotFoundError as e:
    raise SystemExit(
        "Dépendance manquante : 'minio'. Assure-toi d'être dans le venv et d'avoir installé les dépendances (pip install -r requirement.txt)."
    ) from e

RandomForest_GradientBoosting = getattr(__import__("[ia]prediction.RandomForest_GradientBoosting"), "RandomForest_GradientBoosting")
svm = getattr(__import__("[ia]prediction.svm"), "svm")
mlp = getattr(__import__("[ia]prediction.mlp"), "mlp")
decision_tree = getattr(__import__("[ia]prediction.decision_tree"), "decision_tree")
adaboost = getattr(__import__("[ia]prediction.adaboost"), "adaboost")

FILES = {
    "all_indicator": "all_indicator.parquet",
    "all_president": "all_president.parquet"
}

df_indicator = load_module.load_parquet_from_minio(FILES["all_indicator"])
data_quality_module.quality_report(df_indicator, "df_indicator", f"indicator_quality_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json")
df_president = load_module.load_parquet_from_minio(FILES["all_president"])
data_quality_module.quality_report(df_president, "df_president", f"president_quality_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json")

results_rf = RandomForest_GradientBoosting.train_and_predict(df_indicator, df_president, annee_cible=2024)
val_rf = results_rf.get("validation", {})

# Export JSON results (RandomForest + GradientBoosting)
val_rf_r2 = val_rf.get("r2_par_famille", {})
avg_r2 = sum(val_rf_r2.values()) / len(val_rf_r2) if val_rf_r2 else float("nan")
rf_json = _build_model_json(
    model_name="RandomForest_GradientBoosting",
    accuracy=val_rf.get("accuracy_vainqueur_dept", float("nan")),
    mae=val_rf.get("mae_moyen", float("nan")),
    r2=avg_r2,
    classification_report="",
    results_2024={
        "t1_national": results_rf.get("t1_national", {}),
        "t2_national": results_rf.get("t2_national", {}),
        "t1_par_dept": results_rf.get("t1_par_dept", pd.DataFrame()).reset_index(drop=True).to_dict(orient="records")
            if not results_rf.get("t1_par_dept", pd.DataFrame()).empty else [],
        "t2_par_dept": results_rf.get("t2_par_dept", pd.DataFrame()).reset_index(drop=True).to_dict(orient="records")
            if not results_rf.get("t2_par_dept", pd.DataFrame()).empty else [],
    },
    extra={
        "validation": results_rf.get("validation", {}),
        "features": results_rf.get("features", []),
        "rf_importances": {k: list(v) for k, v in results_rf.get("rf_importances", {}).items()},
    },
)
_write_json("RandomForest_GradientBoosting_result.json", rf_json)

accuracy, report, resultats_2024, mae, r2 = svm.train_logistic_model(df_indicator, df_president)
_write_json("svm_result.json", _build_model_json(
    model_name="svm",
    accuracy=accuracy,
    mae=mae,
    r2=r2,
    classification_report=report,
    results_2024=resultats_2024,
))

accuracy, report, resultats_2024, mae, r2 = mlp.train_logistic_model(df_indicator, df_president)
_write_json("mlp_result.json", _build_model_json(
    model_name="mlp",
    accuracy=accuracy,
    mae=mae,
    r2=r2,
    classification_report=report,
    results_2024=resultats_2024,
))

accuracy, report, resultats_2024, mae, r2 = decision_tree.train_logistic_model(df_indicator, df_president)
_write_json("decision_tree_result.json", _build_model_json(
    model_name="decision_tree",
    accuracy=accuracy,
    mae=mae,
    r2=r2,
    classification_report=report,
    results_2024=resultats_2024,
))

accuracy, report, resultats_2024, mae, r2 = adaboost.train_logistic_model(df_indicator, df_president)
_write_json("adaboost_result.json", _build_model_json(
    model_name="adaboost",
    accuracy=accuracy,
    mae=mae,
    r2=r2,
    classification_report=report,
    results_2024=resultats_2024,
))