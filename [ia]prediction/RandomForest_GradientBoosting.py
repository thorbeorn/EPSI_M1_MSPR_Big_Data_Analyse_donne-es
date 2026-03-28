"""
=============================================================
  PRÉDICTION ÉLECTORALE — ENSEMBLE LEARNING (STACKING)
  Tâche    : prédire le % de vote par famille (gauche/centre/droite)
             puis agréger au niveau national

  Architecture :
    Niveau 1 (base learners) :
      - RandomForestRegressor
      - GradientBoostingRegressor
      - SVR (RBF + PCA)
      - MLPRegressor
      - AdaBoostRegressor
      - DecisionTreeRegressor
    Niveau 2 (méta-modèle) :
      - Ridge — apprend à combiner les prédictions du niveau 1

  Avantage du stacking vs simple moyenne :
      Le méta-modèle apprend QUELS modèles faire confiance selon la famille
      et le contexte, plutôt que de les pondérer uniformément.

  Split : 80/20 par département sur données 2017+2022 empilées
  Métriques : accuracy vainqueur dept, MAE, RMSE, R²
=============================================================
"""

import logging
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.decomposition import PCA
from sklearn.base import clone
from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import balanced_accuracy_score
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
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


def _build_dataset(df_indicateurs, df_resultats, annee_cible):
    """Construit le dataset empilé (toutes années < annee_cible)."""
    df_indic = df_indicateurs.copy()
    df_indic["Code_departement"] = _normalize_dept(df_indic["Code_departement"])
    features = [c for c in df_indic.columns if c not in ["Code_departement", "annee"]]
    df_indic[features] = df_indic[features].apply(pd.to_numeric, errors="coerce")

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

    return pd.DataFrame(rows), features, df_indic, annees_indic_dispo


def _make_base_learners(n_features):
    """Retourne la liste des modèles de niveau 1."""
    n_pca = min(15, n_features)
    return [
        ("rf",  RandomForestRegressor(n_estimators=300, max_depth=6, min_samples_leaf=3,
                                      random_state=42, n_jobs=-1)),
        ("gb",  GradientBoostingRegressor(n_estimators=200, max_depth=4, learning_rate=0.05,
                                          subsample=0.8, min_samples_leaf=3, random_state=42)),
        ("ada", AdaBoostRegressor(estimator=DecisionTreeRegressor(max_depth=4),
                                  n_estimators=100, learning_rate=0.5, random_state=42)),
        ("dt",  DecisionTreeRegressor(max_depth=6, min_samples_leaf=3, random_state=42)),
        ("mlp", MLPRegressor(hidden_layer_sizes=(128, 64, 32), activation="relu", solver="adam",
                             max_iter=1000, random_state=42, early_stopping=True,
                             validation_fraction=0.1)),
        # SVR sur espace PCA (géré séparément dans le pipeline)
        ("svr", SVR(kernel="rbf", C=10.0, epsilon=0.1)),
    ]


class _StackingPipeline:
    """
    Pipeline interne qui gère :
    - imputation / scaling / PCA pour le SVR
    - scaling standard pour les autres modèles
    - génération des méta-features via cross-validation
    - entraînement du méta-modèle Ridge
    """

    def __init__(self, n_features, n_splits=5):
        self.n_features  = n_features
        self.n_splits    = n_splits
        self.imputer     = SimpleImputer(strategy="median")
        self.scaler      = StandardScaler()
        self.pca         = PCA(n_components=min(15, n_features), random_state=42)
        self.meta_scaler = StandardScaler()
        self.meta_model  = Ridge(alpha=1.0)
        self.base_names  = []
        self.base_models = []   # modèles entraînés sur 100% pour la prédiction finale

    def _preprocess(self, X_raw, fit=False):
        """Imputation + scaling. Retourne (X_scaled, X_pca)."""
        if fit:
            X_imp = self.imputer.fit_transform(X_raw)
            X_sc  = self.scaler.fit_transform(X_imp)
            X_pca = self.pca.fit_transform(X_sc)
        else:
            X_imp = self.imputer.transform(X_raw)
            X_sc  = self.scaler.transform(X_imp)
            X_pca = self.pca.transform(X_sc)
        return X_sc, X_pca

    def fit_predict_oof(self, X_raw, y):
        """
        Génère les out-of-fold predictions pour construire les méta-features.
        Retourne meta_X (n_samples × n_models) pour entraîner le Ridge.
        """
        X_sc, X_pca = self._preprocess(X_raw, fit=True)
        n = len(y)
        learners = _make_base_learners(self.n_features)
        self.base_names = [name for name, _ in learners]

        oof_preds = np.zeros((n, len(learners)))
        kf = KFold(n_splits=self.n_splits, shuffle=True, random_state=42)

        for fold_i, (tr_idx, te_idx) in enumerate(kf.split(X_sc)):
            for j, (name, model) in enumerate(learners):
                X_tr = X_pca[tr_idx] if name == "svr" else X_sc[tr_idx]
                X_te = X_pca[te_idx] if name == "svr" else X_sc[te_idx]
                model_clone = clone(model)
                model_clone.fit(X_tr, y[tr_idx])
                oof_preds[te_idx, j] = model_clone.predict(X_te)

        # Entraînement méta-modèle
        meta_X = self.meta_scaler.fit_transform(oof_preds)
        self.meta_model.fit(meta_X, y)

        # Entraînement final des base learners sur 100% des données
        self.base_models = []
        for name, model in learners:
            X_fit = X_pca if name == "svr" else X_sc
            m = clone(model)
            m.fit(X_fit, y)
            self.base_models.append((name, m))

        return oof_preds  # pour debug / métriques intermédiaires

    def predict(self, X_raw):
        """Prédiction finale via stacking."""
        X_sc, X_pca = self._preprocess(X_raw, fit=False)
        base_preds = np.zeros((len(X_sc), len(self.base_models)))
        for j, (name, model) in enumerate(self.base_models):
            X_in = X_pca if name == "svr" else X_sc
            base_preds[:, j] = model.predict(X_in)
        meta_X = self.meta_scaler.transform(base_preds)
        return self.meta_model.predict(meta_X)

    def get_meta_weights(self):
        """Retourne les coefficients du méta-modèle (importance de chaque base learner)."""
        return dict(zip(self.base_names, self.meta_model.coef_.tolist()))





def train_and_predict(df_indicateurs, df_resultats, annee_cible=2024):
    """
    Entraîne un stacking ensemble sur 2017+2022 et prédit les résultats 2024.
    Retourne un dict avec toutes les métriques + prédictions 2024.
    """
    logger.info("EnsembleLearning: début train_and_predict")

    df_all, features, df_indic, annees_indic_dispo = _build_dataset(
        df_indicateurs, df_resultats, annee_cible
    )

    familles_cibles = [f"pct_{f}" for f in FAMILLES if f"pct_{f}" in df_all.columns]
    familles_labels = [c.replace("pct_", "") for c in familles_cibles]

    df_all[features]        = df_all[features].replace([np.inf, -np.inf], np.nan)
    df_all[familles_cibles] = df_all[familles_cibles].replace([np.inf, -np.inf], np.nan)

    # ── SPLIT 80/20 PAR DÉPARTEMENT ──────────────────────────────────────────
    depts_uniques = df_all["dept"].unique().tolist()
    depts_train, depts_test = train_test_split(depts_uniques, test_size=0.2, random_state=42)

    df_tr = df_all[df_all["dept"].isin(depts_train)].copy()
    df_te = df_all[df_all["dept"].isin(depts_test)].copy()
    logger.debug(f"EnsembleLearning: lignes train={len(df_tr)}, lignes test={len(df_te)}")

    X_tr_raw = df_tr[features].values.astype(float)
    X_te_raw = df_te[features].values.astype(float)

    # ── STACKING PAR FAMILLE ─────────────────────────────────────────────────
    pipelines    = {}   # un pipeline par famille (pour la prédiction 2024)
    metrics      = {}
    meta_weights = {}

    for col, label in zip(familles_cibles, familles_labels):
        logger.info(f"EnsembleLearning: entraînement famille '{label}'")
        y_tr = df_tr[col].fillna(df_tr[col].median()).values.astype(float)
        y_te = df_te[col].fillna(df_te[col].median()).values.astype(float)

        pipe = _StackingPipeline(n_features=len(features), n_splits=5)
        pipe.fit_predict_oof(X_tr_raw, y_tr)

        y_pred = pipe.predict(X_te_raw)

        metrics[label] = {
            "MAE":  round(float(mean_absolute_error(y_te, y_pred)), 4),
            "RMSE": round(float(np.sqrt(mean_squared_error(y_te, y_pred))), 4),
            "R2":   round(float(r2_score(y_te, y_pred)), 4),
        }
        meta_weights[label] = pipe.get_meta_weights()
        pipelines[label]    = pipe

        logger.info(
            f"  {label}: MAE={metrics[label]['MAE']}, "
            f"RMSE={metrics[label]['RMSE']}, R2={metrics[label]['R2']}"
        )

    # ── ACCURACY : uniquement sur l'année la plus récente du test set ───────────
    annee_max_test = df_te["annee"].max()
    idx_last       = df_te.index[df_te["annee"] == annee_max_test]
    pos_last       = [list(df_te.index).index(i) for i in idx_last]

    pred_te_matrix = np.stack([pipelines[l].predict(X_te_raw) for l in familles_labels], axis=1)
    true_te_matrix = df_te[familles_cibles].fillna(0).values

    pred_last  = pred_te_matrix[pos_last]
    true_last  = true_te_matrix[pos_last]
    vainq_pred = np.array(familles_labels)[pred_last.argmax(axis=1)]
    vainq_reel = np.array(familles_labels)[true_last.argmax(axis=1)]
    accuracy = float(balanced_accuracy_score(vainq_reel, vainq_pred))

    # ── RÉENTRAÎNEMENT SUR 100% AVANT PRÉDICTION 2024 ────────────────────────
    X_full_raw = df_all[features].values.astype(float)
    pipelines_final = {}
    rf_importances  = {}   # importance features extraite du RF dans chaque pipeline final

    for col, label in zip(familles_cibles, familles_labels):
        y_full = df_all[col].fillna(df_all[col].median()).values.astype(float)
        pipe = _StackingPipeline(n_features=len(features), n_splits=5)
        pipe.fit_predict_oof(X_full_raw, y_full)
        pipelines_final[label] = pipe
        # Extraire les importances du RandomForest (base learner "rf")
        for name, model in pipe.base_models:
            if name == "rf":
                rf_importances[label] = model.feature_importances_
                break

    # ── PRÉDICTION 2024 PAR DÉPARTEMENT ──────────────────────────────────────
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
    X_2024_raw = indic_2024.values.astype(float)

    pred_matrix = np.stack(
        [pipelines_final[l].predict(X_2024_raw) for l in familles_labels], axis=1
    )
    pred_matrix = np.clip(pred_matrix, 0, None)
    pred_matrix = pred_matrix / pred_matrix.sum(axis=1, keepdims=True) * 100

    df_pred_dept = pd.DataFrame(
        pred_matrix,
        columns=[f"pct_{l}" for l in familles_labels],
        index=indic_2024.index
    )
    df_pred_dept["vainqueur"] = np.array(familles_labels)[pred_matrix.argmax(axis=1)]

    # ── AGRÉGATION NATIONALE ──────────────────────────────────────────────────
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

    logger.info(
        f"EnsembleLearning: résultat national 2024 → {national} | "
        f"vainqueur={vainqueur_national}"
    )

    return {
        "modele": "StackingEnsemble (RF+GB+AdaBoost+DT+MLP+SVR → Ridge)",
        "annee_cible": annee_cible,
        "validation": {
            "methode": "split 80/20 par département — données 2017+2022 empilées",
            "accuracy_pct": round(accuracy * 100, 2),
            "mae_moyen":  round(float(np.mean([m["MAE"]  for m in metrics.values()])), 4),
            "rmse_moyen": round(float(np.mean([m["RMSE"] for m in metrics.values()])), 4),
            "r2_moyen":   round(float(np.mean([m["R2"]   for m in metrics.values()])), 4),
            "metriques_par_famille": metrics,
            "poids_meta_modele": meta_weights,
        },
        "prediction_nationale_2024": national,
        "vainqueur_national_2024": vainqueur_national,
        "prediction_par_dept_2024": df_pred_dept,

    }
