"""
=============================================================
  PRÉDICTION ÉLECTORALE — GRADIENT BOOSTING (RÉGRESSION)
  Tâche    : prédire le % de vote par famille (gauche/centre/droite)
             puis agréger au niveau national
  Train    : 2017 + 2022 empilés — split 80/20 par département
  Prédiction finale : 2024 (indicateurs sociopolitiques réels)
  Métriques : accuracy vainqueur dept, MAE, RMSE, R²
=============================================================
"""

import logging
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error, balanced_accuracy_score
import os
import warnings
warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)
FAMILLES = ["centre", "droite", "gauche"]


def _normalize_dept(series):
    def _fix(code):
        code = str(code).strip()
        return code.zfill(2) if code.isdigit() and len(code) == 1 else code
    return series.apply(_fix)


def train_and_predict(df_indicateurs, df_resultats, annee_cible=2024):
    """
    Pipeline identique à Random Forest mais avec GradientBoostingRegressor.
    1. Empile 2017 + 2022 (indicateurs + % vote T1 par département)
    2. Split 80/20 sur les départements pour évaluation honnête
    3. Entraîne GradientBoostingRegressor par famille politique
    4. Réentraîne sur 100% des données pour la prédiction finale
    5. Prédit les % par département pour 2024
    6. Agrège au niveau national (pondéré population si dispo)
    Retourne un dict avec toutes les métriques + prédictions 2024.
    """
    logger.info("GradientBoosting: début train_and_predict")

    # ── 1. PRÉPARATION INDICATEURS ────────────────────────────────────────────
    df_indic = df_indicateurs.copy()
    df_indic["Code_departement"] = _normalize_dept(df_indic["Code_departement"])
    features = [c for c in df_indic.columns if c not in ["Code_departement", "annee"]]
    df_indic[features] = df_indic[features].apply(pd.to_numeric, errors="coerce")

    # ── 2. PRÉPARATION RÉSULTATS T1 ───────────────────────────────────────────
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

    # ── 3. CONSTRUCTION DU DATASET EMPILÉ (toutes années < annee_cible) ──────
    annees_pres = sorted(df_resultats["annee"].unique())
    annees_train = [a for a in annees_pres if a < annee_cible]
    annees_indic_dispo = sorted(df_indic["annee"].unique())

    rows = []
    for annee in annees_train:
        scores = get_scores_t1(annee)
        familles_dispo = [f for f in FAMILLES if f in scores.columns]

        best_year = max([y for y in annees_indic_dispo if y <= annee], default=annees_indic_dispo[0])
        indic = df_indic[df_indic["annee"] == best_year].set_index("Code_departement")[features]
        indic = indic.apply(pd.to_numeric, errors="coerce")

        depts = indic.index.intersection(scores.index)
        for dept in depts:
            row = {"annee": annee, "dept": dept}
            for f in features:
                row[f] = indic.loc[dept, f] if dept in indic.index else np.nan
            for f in familles_dispo:
                row[f"pct_{f}"] = scores.loc[dept, f] if dept in scores.index else np.nan
            rows.append(row)

    df_all = pd.DataFrame(rows)
    logger.debug(f"GradientBoosting: dataset empilé shape={df_all.shape}")

    familles_cibles = [f"pct_{f}" for f in FAMILLES if f"pct_{f}" in df_all.columns]
    familles_labels = [c.replace("pct_", "") for c in familles_cibles]

    df_all[features]        = df_all[features].replace([np.inf, -np.inf], np.nan)
    df_all[familles_cibles] = df_all[familles_cibles].replace([np.inf, -np.inf], np.nan)

    # ── 4. SPLIT 80/20 SUR LES DÉPARTEMENTS ──────────────────────────────────
    depts_uniques = df_all["dept"].unique().tolist()
    depts_train, depts_test = train_test_split(depts_uniques, test_size=0.2, random_state=42)

    df_tr = df_all[df_all["dept"].isin(depts_train)].copy()
    df_te = df_all[df_all["dept"].isin(depts_test)].copy()
    logger.debug(f"GradientBoosting: lignes train={len(df_tr)}, lignes test={len(df_te)}")

    # ── 5. IMPUTATION + STANDARDISATION ──────────────────────────────────────
    imputer = SimpleImputer(strategy="median")
    scaler  = StandardScaler()

    X_tr = scaler.fit_transform(imputer.fit_transform(df_tr[features]))
    X_te = scaler.transform(imputer.transform(df_te[features]))

    # ── 6. ENTRAÎNEMENT + ÉVALUATION PAR FAMILLE ─────────────────────────────
    models_eval = {}
    metrics = {}

    for col, label in zip(familles_cibles, familles_labels):
        y_tr = df_tr[col].fillna(df_tr[col].median()).values
        y_te = df_te[col].fillna(df_te[col].median()).values

        gbr = GradientBoostingRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, min_samples_leaf=3, random_state=42
        )
        gbr.fit(X_tr, y_tr)
        y_pred = gbr.predict(X_te)

        models_eval[label] = gbr
        metrics[label] = {
            "MAE":  round(float(mean_absolute_error(y_te, y_pred)), 4),
            "RMSE": round(float(np.sqrt(mean_squared_error(y_te, y_pred))), 4),
            "R2":   round(float(r2_score(y_te, y_pred)), 4),
        }
        logger.info(f"  GB {label}: MAE={metrics[label]['MAE']}, R2={metrics[label]['R2']}")

    # ── ACCURACY : uniquement sur l'année la plus récente du test set ───────────
    # On filtre sur la dernière année pour éviter de compter deux fois
    # les depts stables (même vainqueur 2017 et 2022 → double comptage).
    annee_max_test = df_te["annee"].max()
    df_te_last     = df_te[df_te["annee"] == annee_max_test]
    idx_last       = df_te.index[df_te["annee"] == annee_max_test]
    pos_last       = [list(df_te.index).index(i) for i in idx_last]

    pred_te_matrix = np.stack([models_eval[l].predict(X_te) for l in familles_labels], axis=1)
    true_te_matrix = df_te[familles_cibles].fillna(0).values

    pred_last = pred_te_matrix[pos_last]
    true_last = true_te_matrix[pos_last]

    vainq_pred = np.array(familles_labels)[pred_last.argmax(axis=1)]
    vainq_reel = np.array(familles_labels)[true_last.argmax(axis=1)]

    accuracy = float(balanced_accuracy_score(vainq_reel, vainq_pred))

    # ── 7. RÉENTRAÎNEMENT SUR 100% DES DONNÉES ───────────────────────────────
    X_full = scaler.fit_transform(
        imputer.fit_transform(df_all[features].replace([np.inf, -np.inf], np.nan))
    )
    models_final = {}
    for col, label in zip(familles_cibles, familles_labels):
        y_full = df_all[col].fillna(df_all[col].median()).values
        gbr = GradientBoostingRegressor(n_estimators=200, max_depth=4, learning_rate=0.05,
                                        subsample=0.8, min_samples_leaf=3, random_state=42)
        gbr.fit(X_full, y_full)
        models_final[label] = gbr

    # ── 8. PRÉDICTION 2024 PAR DÉPARTEMENT ───────────────────────────────────
    best_year_2024 = max(
        [y for y in annees_indic_dispo if y <= annee_cible],
        default=annees_indic_dispo[-1]
    )
    indic_2024 = (
        df_indic[df_indic["annee"] == best_year_2024]
        .set_index("Code_departement")[features]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
    )

    X_2024 = scaler.transform(imputer.transform(indic_2024.values))

    pred_matrix = np.stack(
        [models_final[l].predict(X_2024) for l in familles_labels], axis=1
    )
    pred_matrix = np.clip(pred_matrix, 0, None)
    pred_matrix = pred_matrix / pred_matrix.sum(axis=1, keepdims=True) * 100

    df_pred_dept = pd.DataFrame(
        pred_matrix,
        columns=[f"pct_{l}" for l in familles_labels],
        index=indic_2024.index
    )
    df_pred_dept["vainqueur"] = np.array(familles_labels)[pred_matrix.argmax(axis=1)]

    # ── 9. AGRÉGATION NATIONALE (pondérée population si dispo) ───────────────
    pop_col = "[compte_publique]population"
    if pop_col in indic_2024.columns:
        pop = indic_2024[pop_col].fillna(indic_2024[pop_col].median()).values
    else:
        pop = np.ones(len(indic_2024))

    national = {
        label: round(float(np.average(pred_matrix[:, i], weights=pop)), 2)
        for i, label in enumerate(familles_labels)
    }
    vainqueur_national = max(national, key=national.get)

    logger.info(f"GradientBoosting: résultat national 2024 → {national} | vainqueur={vainqueur_national}")

    # ── CSV 1 : importance des critères (feature importance GB) ──────────────
    # Moyenne des importances sur les 3 familles
    import numpy as _np2
    imp_moy = _np2.zeros(len(features))
    for m_final in models_final.values():
        imp_moy += m_final.feature_importances_
    imp_moy /= max(len(models_final), 1)

    df_importance = pd.DataFrame({"critere": list(features), "importance_moyenne": imp_moy.round(6)})
    for label, m_final in models_final.items():
        df_importance[f"importance_{label}"] = m_final.feature_importances_.round(6)
    df_importance = df_importance.sort_values("importance_moyenne", ascending=False).reset_index(drop=True)
    df_importance.insert(0, "rang", range(1, len(df_importance) + 1))

    # ── CSV 2 : résultats par famille (national + par département) ────────────
    dept_reset = df_pred_dept.reset_index().rename(columns={"Code_departement": "departement"})
    if "index" in dept_reset.columns:
        dept_reset = dept_reset.rename(columns={"index": "departement"})
    dept_reset["niveau"] = "departement"
    row_nat = {"departement": "FRANCE", "niveau": "national", "vainqueur": vainqueur_national}
    for label in familles_labels:
        row_nat[f"pct_{label}"] = national[label]
    df_resultats_csv = pd.concat([pd.DataFrame([row_nat]), dept_reset], ignore_index=True)

    export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "[ia]exports")
    os.makedirs(export_dir, exist_ok=True)
    path_importance = os.path.join(export_dir, "gb_importance_criteres.csv")
    path_resultats  = os.path.join(export_dir, "gb_resultats_par_famille.csv")
    df_importance.to_csv(path_importance,  index=False, sep=";")
    df_resultats_csv.to_csv(path_resultats, index=False, sep=";")
    logger.info(f"GradientBoosting: CSV importance → {path_importance}")
    logger.info(f"GradientBoosting: CSV résultats  → {path_resultats}")

    return {
        "modele": "GradientBoostingRegressor",
        "annee_cible": annee_cible,
        "validation": {
            "methode": "split 80/20 par département — données 2017+2022 empilées",
            "accuracy_pct": round(accuracy * 100, 2),
            "mae_moyen":  round(float(np.mean([m["MAE"]  for m in metrics.values()])), 4),
            "rmse_moyen": round(float(np.mean([m["RMSE"] for m in metrics.values()])), 4),
            "r2_moyen":   round(float(np.mean([m["R2"]   for m in metrics.values()])), 4),
            "metriques_par_famille": metrics,
        },
        "prediction_nationale_2024": national,
        "vainqueur_national_2024": vainqueur_national,
        "prediction_par_dept_2024": df_pred_dept,
        "csv_importance": path_importance,
        "csv_resultats":  path_resultats,
    }
