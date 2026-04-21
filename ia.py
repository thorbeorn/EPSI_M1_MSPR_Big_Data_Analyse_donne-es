import json
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd

# ── CONFIGURATION DU LOGGING ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ETL_PIPELINE")


# ── UTILITAIRES ───────────────────────────────────────────────────────────────

def _get_exports_dir():
    exports_dir = os.path.join(os.path.dirname(__file__), "[ia]exports")
    os.makedirs(exports_dir, exist_ok=True)
    return exports_dir


def _to_json_serializable(obj):
    """Convertit recursively les types numpy/pandas en types Python natifs."""
    if isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, (np.ndarray, list, tuple)):
        return [_to_json_serializable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, pd.DataFrame):
        return obj.reset_index(drop=True).to_dict(orient="records")
    return obj


def _write_json(filename: str, data):
    path = os.path.join(_get_exports_dir(), filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_to_json_serializable(data), f, ensure_ascii=False, indent=2)
    logger.info(f"JSON exporte : {path}")


def _build_result_json(model_name: str, result: dict) -> dict:
    """
    Structure JSON standardisee pour tous les modeles.
    Tous les modeles retournent un dict avec :
      - validation  (accuracy, mae, rmse, r2 + par famille)
      - prediction_nationale_2024  {centre, droite, gauche}
      - vainqueur_national_2024
      - prediction_par_dept_2024  (DataFrame)
    """
    val = result.get("validation", {})
    nat = result.get("prediction_nationale_2024", {})

    return {
        "model":        model_name,
        "generated_at": datetime.now().isoformat(),
        "metrics": {
            "accuracy_pct": val.get("accuracy_pct"),
            "mae_moyen":             val.get("mae_moyen"),
            "rmse_moyen":            val.get("rmse_moyen"),
            "r2_moyen":              val.get("r2_moyen"),
            "metriques_par_famille": val.get("metriques_par_famille", {}),
        },
        "resultat_national_2024": {
            "centre":    nat.get("centre"),
            "droite":    nat.get("droite"),
            "gauche":    nat.get("gauche"),
            "vainqueur": result.get("vainqueur_national_2024"),
        },
        "resultats_par_dept_2024": result.get("prediction_par_dept_2024", pd.DataFrame()),
        "methode_validation": val.get("methode", ""),
    }


# ── CHARGEMENT DES MODULES ────────────────────────────────────────────────────

try:
    load_module         = getattr(__import__("[ia]prediction.load"),          "load")
    data_quality_module = getattr(__import__("[ia]prediction.data_quality"),  "data_quality")
except ModuleNotFoundError as e:
    raise SystemExit(
        "Dependance manquante : assure-toi d'etre dans le venv "
        "(pip install -r requirement.txt)."
    ) from e

svm = getattr(
    __import__("[ia]prediction.svm"), "svm")
mlp = getattr(
    __import__("[ia]prediction.mlp"), "mlp")
decision_tree = getattr(
    __import__("[ia]prediction.decision_tree"), "decision_tree")
adaboost = getattr(
    __import__("[ia]prediction.adaboost"), "adaboost")
prediction_random_forest = getattr(
    __import__("[ia]prediction.prediction_random_forest"), "prediction_random_forest")
prediction_gradient_boosting = getattr(
    __import__("[ia]prediction.prediction_gradient_boosting"), "prediction_gradient_boosting")

# ── CHARGEMENT DES DONNEES ────────────────────────────────────────────────────

df_indicator = load_module.load_parquet_from_minio("all_indicator.parquet")
data_quality_module.quality_report(
    df_indicator, "df_indicator",
    f"indicator_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
)

df_president = load_module.load_parquet_from_minio("all_president.parquet")
data_quality_module.quality_report(
    df_president, "df_president",
    f"president_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
)

# ── SVM ───────────────────────────────────────────────────────────────────────

results_svm = svm.train_logistic_model(df_indicator, df_president)
_write_json("svm_result.json", _build_result_json("SVM", results_svm))

# ── MLP ───────────────────────────────────────────────────────────────────────

results_mlp = mlp.train_logistic_model(df_indicator, df_president)
_write_json("mlp_result.json", _build_result_json("MLP", results_mlp))

# ── DECISION TREE ─────────────────────────────────────────────────────────────

results_dt = decision_tree.train_logistic_model(df_indicator, df_president)
_write_json("decision_tree_result.json", _build_result_json("DecisionTree", results_dt))

# ── ADABOOST ──────────────────────────────────────────────────────────────────

results_ada = adaboost.train_logistic_model(df_indicator, df_president)
_write_json("adaboost_result.json", _build_result_json("AdaBoost", results_ada))

# ── RANDOM FOREST ────────────────────────────────────────────────────────

results_rf = prediction_random_forest.train_and_predict(
    df_indicator, df_president, annee_cible=2024
)
_write_json("random_forest_result.json", _build_result_json("RandomForest", results_rf))

# ── GRADIENT BOOSTING ────────────────────────────────────────────────────

results_gbr = prediction_gradient_boosting.train_and_predict(
    df_indicator, df_president, annee_cible=2024
)
_write_json("gradient_boosting_result.json", _build_result_json("GradientBoosting", results_gbr))
logger.info(f"CSV importance GB  → {results_gbr.get('csv_importance')}")
logger.info(f"CSV résultats GB   → {results_gbr.get('csv_resultats')}")

# ── RECAP CONSOLE ─────────────────────────────────────────────────────────────

logger.info("=" * 70)
logger.info("RECAPITULATIF DES MODELES")
logger.info("=" * 70)

all_results = {
    "SVM":              results_svm,
    "MLP":              results_mlp,
    "DecisionTree":     results_dt,
    "AdaBoost":         results_ada,
    "RandomForest":     results_rf,
    "GradientBoosting": results_gbr,
}

for name, res in all_results.items():
    val = res.get("validation", {})
    nat = res.get("prediction_nationale_2024", {})
    logger.info(
        f"{name:25s} | "
        f"accuracy={str(val.get('accuracy_pct','?')):>5}% | "
        f"MAE={str(val.get('mae_moyen','?')):>6} | "
        f"R2={str(val.get('r2_moyen','?')):>6} | "
        f"centre={nat.get('centre','?')}% "
        f"droite={nat.get('droite','?')}% "
        f"gauche={nat.get('gauche','?')}%"
    )
