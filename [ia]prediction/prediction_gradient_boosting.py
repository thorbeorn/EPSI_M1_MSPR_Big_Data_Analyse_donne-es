"""
=============================================================
  PRÉDICTION ÉLECTORALE — GRADIENT BOOSTING UNIQUEMENT
  Train : année n-1  |  Test : dernière année disponible
  Métriques retournées : accuracy, mae_moyen, r2_moyen
=============================================================
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
import warnings
warnings.filterwarnings("ignore")

FAMILLES = ["centre", "droite", "gauche"]


def _normalize_dept(series):
    def _fix(code):
        code = str(code).strip()
        return code.zfill(2) if code.isdigit() and len(code) == 1 else code
    return series.apply(_fix)


def train_and_predict(df_indicateurs, df_resultats, annee_cible=2024):
    # ── Préparation indicateurs ───────────────────────────────────────────────
    df_indic = df_indicateurs.copy()
    df_indic["Code_departement"] = _normalize_dept(df_indic["Code_departement"])
    features = [c for c in df_indic.columns if c not in ["Code_departement", "annee"]]
    df_indic[features] = df_indic[features].apply(pd.to_numeric, errors="coerce")

    annees_indic = sorted(df_indic["annee"].unique())

    def get_best_indic(annee):
        available = [y for y in annees_indic if y <= annee]
        best = max(available) if available else annees_indic[0]
        sub = df_indic[df_indic["annee"] == best].set_index("Code_departement")[features]
        return sub.apply(pd.to_numeric, errors="coerce")

    # ── Préparation scores électoraux T1 ─────────────────────────────────────
    df_pres = df_resultats.copy()
    df_pres["code_departement"] = _normalize_dept(df_pres["code_departement"])
    df_pres["[president_sortant]pourcentage"] = pd.to_numeric(
        df_pres["[president_sortant]pourcentage"], errors="coerce"
    )
    df_t1 = df_pres[df_pres["[president_sortant]tour"] == "t1"]

    def get_scores_t1(annee):
        s = df_t1[df_t1["annee"] == annee]
        pivot = s.pivot_table(
            index="code_departement",
            columns="[president_sortant]famille_politique",
            values="[president_sortant]pourcentage",
            aggfunc="mean",
        )
        pivot.index = pivot.index.astype(str).str.strip()
        return pivot

    annees_pres = sorted(df_resultats["annee"].unique())
    annee_train = annees_pres[-2]   # ex : 2017
    annee_test  = annees_pres[-1]   # ex : 2022

    scores_train = get_scores_t1(annee_train)
    scores_test  = get_scores_t1(annee_test)

    familles = [f for f in FAMILLES if f in scores_train.columns and f in scores_test.columns]

    # ── Alignement des départements ───────────────────────────────────────────
    indic_train = get_best_indic(annee_train)
    indic_test  = get_best_indic(annee_test)

    depts = (
        indic_train.index
        .intersection(indic_test.index)
        .intersection(scores_train.index)
        .intersection(scores_test.index)
    )

    X_train   = indic_train.loc[depts].fillna(0).values
    X_test    = indic_test.loc[depts].fillna(0).values
    scaler    = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s  = scaler.transform(X_test)

    # ── Entraînement et test — Gradient Boosting ─────────────────────────────
    mae_val  = {}
    r2_val   = {}
    pred_val = {}

    for f in familles:
        y_train = scores_train.loc[depts, f].fillna(0).values
        y_test  = scores_test.loc[depts, f].fillna(0).values

        gbr = GradientBoostingRegressor(
            n_estimators=200,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42
        )
        gbr.fit(X_train_s, y_train)
        p = gbr.predict(X_test_s)

        pred_val[f] = p
        mae_val[f]  = mean_absolute_error(y_test, p)
        r2_val[f]   = r2_score(y_test, p)

    # ── Accuracy vainqueur par département ───────────────────────────────────
    pred_matrix = np.stack([pred_val[f] for f in familles], axis=1)
    vainq_pred  = np.array(familles)[pred_matrix.argmax(axis=1)]
    vainq_reel  = scores_test.loc[depts, familles].idxmax(axis=1).values
    accuracy    = float(np.mean(vainq_pred == vainq_reel))

    return {
        "validation": {
            "accuracy_vainqueur_dept": accuracy,
            "mae_moyen": float(np.mean(list(mae_val.values()))),
            "r2_moyen":  float(np.mean(list(r2_val.values()))),
        }
    }
