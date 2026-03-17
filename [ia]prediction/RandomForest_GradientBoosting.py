"""
=============================================================
  PRÉDICTION ÉLECTORALE FRANÇAISE — MODÈLE GÉNÉRIQUE
  Callable depuis un programme principal.

  Usage :
      import prediction_president_2024 as ia
      results = ia.train_and_predict(df_indicator, df_president)

  Pipeline en 2 étapes :

  ÉTAPE 1 — Prédire chaque critère socio-éco pour l'année cible
            Régression Ridge par critère, entraînée sur
            (annee_A → annee_B), appliquée sur annee_B → annee_cible.

  ÉTAPE 2 — Prédire le % de vote par famille politique
            (gauche / centre / droite) au T1 et T2
            Entraînement sur TOUTES les années disponibles.
            Input  : critères socio-éco prédits pour l'année cible
            Target : % de vote historiques

  Validation honnête : entraîné sur n-1 années, testé sur la dernière.
=============================================================
"""

import logging
import os
import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
import warnings
warnings.filterwarnings("ignore")

# CONFIGURATION DU LOGGING
logger = logging.getLogger(__name__)

FAMILLES    = ["centre", "droite", "gauche"]
FAMILLES_T2 = ["centre", "droite"]
POP_COL     = "[compte_publique]population"


# ─────────────────────────────────────────────────────────────────────────────
# UTILITAIRES INTERNES
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_dept(series: pd.Series) -> pd.Series:
    """
    Normalise les codes département en format cohérent.
    '1' → '01', '01' → '01', '2A' → '2A', '971' → '971'
    Règle : code numérique à 1 chiffre → zfill(2), sinon on garde tel quel.
    """
    def _fix(code):
        code = str(code).strip()
        if code.isdigit() and len(code) == 1:
            return code.zfill(2)
        return code
    return series.apply(_fix)


def _prep_indicator(df_indicateurs):
    """Nettoie et retourne le DataFrame indicateurs indexé par (annee, dept)."""
    df = df_indicateurs.copy()
    df["Code_departement"] = _normalize_dept(df["Code_departement"])
    features = [c for c in df.columns if c not in ["Code_departement", "annee"]]
    df[features] = df[features].apply(pd.to_numeric, errors="coerce")
    return df, features


def _prep_president(df_resultats, tour="t1"):
    """Retourne un pivot dept × famille pour un tour et une année donnés."""
    df = df_resultats.copy()
    df["code_departement"] = _normalize_dept(df["code_departement"])
    df["[president_sortant]pourcentage"] = pd.to_numeric(
        df["[president_sortant]pourcentage"], errors="coerce"
    )
    sub = df[df["[president_sortant]tour"] == tour]

    def get_scores(annee):
        s = sub[sub["annee"] == annee]
        pivot = s.pivot_table(
            index="code_departement",
            columns="[president_sortant]famille_politique",
            values="[president_sortant]pourcentage",
            aggfunc="mean",
        )
        pivot.index = pivot.index.astype(str).str.strip()
        return pivot

    return sub, get_scores


def _predict_indicators_for_year(df_prev, df_curr, features):
    """
    Étape 1 : Ridge par critère.
    Entraîné sur (df_prev → df_curr), appliqué sur df_curr → prédiction suivante.
    Retourne (df_pred, dict_r2).
    """
    depts = df_prev.index.intersection(df_curr.index)
    df_prev = df_prev.loc[depts]
    df_curr = df_curr.loc[depts]

    df_pred  = pd.DataFrame(index=depts)
    r2_scores = {}

    for critere in features:
        X = df_prev[critere].values.reshape(-1, 1)
        y = df_curr[critere].values
        reg = Ridge(alpha=1.0)
        reg.fit(X, y)
        ss_res = np.sum((y - reg.predict(X)) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2_scores[critere] = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
        df_pred[critere] = reg.predict(df_curr[critere].values.reshape(-1, 1))

    return df_pred.loc[depts], r2_scores


def _build_vote_model(X_train_s, y_train):
    """Voting ensemble RF + GBR."""
    rf  = RandomForestRegressor(n_estimators=300, max_depth=5,
                                 random_state=42, n_jobs=-1)
    gbr = GradientBoostingRegressor(n_estimators=200, max_depth=3,
                                     learning_rate=0.05, subsample=0.8,
                                     random_state=42)
    rf.fit(X_train_s, y_train)
    gbr.fit(X_train_s, y_train)
    return rf, gbr


def _ensemble_predict(rf, gbr, X_s):
    return (rf.predict(X_s) * 2 + gbr.predict(X_s)) / 3


def _normalize_scores(pred_dict, familles):
    """Normalise les scores pour que la somme = 100 % par département."""
    total = sum(pred_dict[f] for f in familles)
    return {f: pred_dict[f] / total * 100 for f in familles}


def _weighted_national(pred_dict, familles, weights):
    """Score national pondéré par population."""
    total_w = weights.sum()
    return {f: (pred_dict[f] * weights).sum() / total_w for f in familles}


# ─────────────────────────────────────────────────────────────────────────────
# FONCTION PRINCIPALE — CALLABLE DEPUIS LE MAIN
# ─────────────────────────────────────────────────────────────────────────────

def train_and_predict(df_indicateurs, df_resultats, annee_cible=2024, verbose=True):
    """
    Entraîne le modèle sur toutes les données historiques disponibles
    (résultats présidentiels + indicateurs) et prédit les scores politiques
    pour l'année cible (T1 + T2 si applicable).

    Paramètres
    ----------
    df_indicateurs : pd.DataFrame
        Colonnes attendues : Code_departement, annee, + tous les critères.
        Doit contenir au moins les années 2017, 2022 et annee_cible.
    df_resultats : pd.DataFrame
        Colonnes attendues : annee, code_departement,
        [president_sortant]tour, [president_sortant]famille_politique,
        [president_sortant]pourcentage.
        Doit contenir les années 2017 et 2022.
    annee_cible : int, optional (défaut 2024)
        Année pour laquelle on prédit les résultats.
    verbose : bool, optional (défaut True)
        Affiche les logs détaillés.

    Retourne
    --------
    dict avec les clés :
        "t1_national"  : dict  {famille: score_%}
        "t1_par_dept"  : pd.DataFrame
        "t2_national"  : dict  {famille: score_%}  (si applicable)
        "t2_par_dept"  : pd.DataFrame  (si applicable)
        "validation"   : dict  (métriques MAE / accuracy)
    """

    logger.info("RandomForest_GradientBoosting: début de train_and_predict")

    # ── 1. PRÉPARATION ──────────────────────────────────────────────────────
    df_indic, FEATURES = _prep_indicator(df_indicateurs)

    # Années d'indicateurs disponibles (hors cible)
    annees_indic = sorted(df_indic["annee"].unique())
    annees_hist  = [a for a in annees_indic if a < annee_cible]
    if len(annees_hist) < 2:
        raise ValueError(
            f"Il faut au moins 2 années d'indicateurs antérieures à {annee_cible}. "
            f"Disponibles : {annees_hist}"
        )
    annee_prev = annees_hist[-2]
    annee_curr = annees_hist[-1]
    logger.debug(f"RandomForest_GradientBoosting: années utilisées (prev={annee_prev}, curr={annee_curr})")

    def get_indic(a):
        sub = df_indic[df_indic["annee"] == a].set_index("Code_departement")[FEATURES]
        return sub.apply(pd.to_numeric, errors="coerce")

    df_prev = get_indic(annee_prev)
    df_curr = get_indic(annee_curr)
    df_tgt  = get_indic(annee_cible) if annee_cible in annees_indic else None

    # Années présidentielles disponibles
    annees_pres = sorted(df_resultats["annee"].unique())

    _, get_t1 = _prep_president(df_resultats, tour="t1")
    _, get_t2 = _prep_president(df_resultats, tour="t2")

    # Scores par tour et par année
    scores_t1 = {a: get_t1(a) for a in annees_pres}
    scores_t2 = {a: get_t2(a) for a in annees_pres}

    # Intersection des départements communs à TOUTES les années présidentielles
    depts_all = df_prev.index.copy()
    for a in annees_pres:
        for s in [scores_t1[a], scores_t2[a]]:
            if not s.empty:
                depts_all = depts_all.intersection(s.index)
    depts_all = depts_all.intersection(df_curr.index)
    if df_tgt is not None:
        depts_all = depts_all.intersection(df_tgt.index)

    # Restreindre
    df_prev = df_prev.loc[depts_all].fillna(df_prev.median())
    df_curr = df_curr.loc[depts_all].fillna(df_curr.median())
    if df_tgt is not None:
        df_tgt = df_tgt.loc[depts_all].fillna(df_tgt.median())

    logger.debug(f"RandomForest_GradientBoosting: départements retenus = {len(depts_all)}")

    for a in annees_pres:
        scores_t1[a] = scores_t1[a].loc[depts_all]
        if not scores_t2[a].empty:
            scores_t2[a] = scores_t2[a].reindex(depts_all)

    # Vérifier familles T1 disponibles
    fam_t1_dispo = set()
    for a in annees_pres:
        fam_t1_dispo |= set(scores_t1[a].columns.tolist())
    FAMILLES_T1 = [f for f in FAMILLES if f in fam_t1_dispo]

    fam_t2_dispo = set()
    for a in annees_pres:
        if not scores_t2[a].empty:
            fam_t2_dispo |= set(scores_t2[a].columns.tolist())
    FAMILLES_T2_ACT = [f for f in FAMILLES_T2 if f in fam_t2_dispo]
    has_t2 = len(FAMILLES_T2_ACT) >= 2

    # ── 2. ÉTAPE 1 — PROJECTION DES INDICATEURS VERS L'ANNÉE CIBLE ──────────
    if df_tgt is not None:
        # Les données réelles de l'année cible existent → on les utilise directement
        df_cible = df_tgt
        r2_info = None
    else:
        df_cible, r2_info = _predict_indicators_for_year(df_prev, df_curr, FEATURES)
        r2_series = pd.Series(r2_info)

    df_cible = df_cible.loc[depts_all].fillna(0)

    # ── 3. ÉTAPE 2 — MODÈLE DE VOTE T1 ──────────────────────────────────────
    # Construction du dataset d'entraînement :
    # on empile les indicateurs de chaque année présidentielle
    # avec les scores correspondants
    def get_indic_for_pres_year(a):
        """
        Pour une année présidentielle, on utilise l'indicateur le plus proche
        disponible dans df_indic (même année ou inférieure).
        """
        available = [y for y in annees_indic if y <= a]
        if not available:
            available = annees_indic
        best = max(available)
        sub = df_indic[df_indic["annee"] == best].set_index("Code_departement")[FEATURES]
        sub = sub.reindex(depts_all).apply(pd.to_numeric, errors="coerce").fillna(0)
        return sub

    X_parts  = []
    y_parts_t1 = {f: [] for f in FAMILLES_T1}

    for a in annees_pres:
        indic_a = get_indic_for_pres_year(a)
        sc_t1_a = scores_t1[a].reindex(depts_all)[FAMILLES_T1].fillna(0)
        X_parts.append(indic_a.values)
        for f in FAMILLES_T1:
            y_parts_t1[f].append(sc_t1_a[f].values)

    X_train_t1 = np.vstack(X_parts)
    scaler_t1  = StandardScaler()
    X_train_t1_s = scaler_t1.fit_transform(X_train_t1)
    X_cible_t1_s = scaler_t1.transform(df_cible.values)

    pred_t1 = {}
    rf_importances = {}
    for f in FAMILLES_T1:
        y_tr = np.concatenate(y_parts_t1[f])
        rf, gbr = _build_vote_model(X_train_t1_s, y_tr)
        pred_t1[f] = _ensemble_predict(rf, gbr, X_cible_t1_s)
        rf_importances[f] = rf.feature_importances_

    pred_t1_norm = _normalize_scores(pred_t1, FAMILLES_T1)

    # Score national T1 pondéré par population
    pop = df_curr[POP_COL].loc[depts_all] if POP_COL in df_curr.columns else pd.Series(
        np.ones(len(depts_all)), index=depts_all
    )
    score_nat_t1 = _weighted_national(pred_t1_norm, FAMILLES_T1, pop.values)

    # Résultats par département T1
    df_res_t1 = pd.DataFrame({"departement": depts_all}, index=depts_all)
    for f in FAMILLES_T1:
        df_res_t1[f"score_{f}"] = pred_t1_norm[f].round(2)
    df_res_t1["vainqueur"] = df_res_t1[
        [f"score_{f}" for f in FAMILLES_T1]
    ].idxmax(axis=1).str.replace("score_", "")

    # ── 4. ÉTAPE 2 — MODÈLE DE VOTE T2 ──────────────────────────────────────
    df_res_t2       = pd.DataFrame()
    score_nat_t2    = {}

    if has_t2:
        # Familles qualifiées pour le T2 = les 2 premières du T1 prédit
        top2 = sorted(score_nat_t1.items(), key=lambda x: -x[1])[:2]
        familles_qualifiees = [f for f, _ in top2]

        # Ne garder que les familles présentes dans les données T2 historiques
        familles_t2_eff = [f for f in familles_qualifiees if f in FAMILLES_T2_ACT]
        if len(familles_t2_eff) < 2:
            familles_t2_eff = FAMILLES_T2_ACT

        X_parts_t2  = []
        y_parts_t2  = {f: [] for f in familles_t2_eff}

        for a in annees_pres:
            if scores_t2[a].empty:
                continue
            sc_t2_a = scores_t2[a].reindex(depts_all)
            cols_ok = [f for f in familles_t2_eff if f in sc_t2_a.columns]
            if not cols_ok:
                continue
            indic_a = get_indic_for_pres_year(a)
            X_parts_t2.append(indic_a.values)
            for f in familles_t2_eff:
                col = sc_t2_a[f].fillna(0).values if f in sc_t2_a.columns else np.zeros(len(depts_all))
                y_parts_t2[f].append(col)

        if X_parts_t2:
            X_train_t2   = np.vstack(X_parts_t2)
            scaler_t2    = StandardScaler()
            X_train_t2_s = scaler_t2.fit_transform(X_train_t2)
            X_cible_t2_s = scaler_t2.transform(df_cible.values)

            pred_t2 = {}
            for f in familles_t2_eff:
                y_tr = np.concatenate(y_parts_t2[f])
                rf, gbr = _build_vote_model(X_train_t2_s, y_tr)
                pred_t2[f] = _ensemble_predict(rf, gbr, X_cible_t2_s)

            pred_t2_norm = _normalize_scores(pred_t2, familles_t2_eff)
            score_nat_t2 = _weighted_national(pred_t2_norm, familles_t2_eff, pop.values)

            df_res_t2 = pd.DataFrame({"departement": depts_all}, index=depts_all)
            for f in familles_t2_eff:
                df_res_t2[f"score_{f}"] = pred_t2_norm[f].round(2)
            df_res_t2["vainqueur"] = df_res_t2[
                [f"score_{f}" for f in familles_t2_eff]
            ].idxmax(axis=1).str.replace("score_", "")

    # ── 5. VALIDATION — ENTRAÎNÉ SUR n-1 ANNÉES, TESTÉ SUR LA DERNIÈRE ──────
    annee_test  = annees_pres[-1]
    annees_train_val = annees_pres[:-1]
    logger.debug(f"RandomForest_GradientBoosting: validation sur annee_test={annee_test}")

    val_results = {}

    if annees_train_val:
        # Build val training set (tout sauf l'année test)
        X_val_parts = []
        y_val_parts_t1 = {f: [] for f in FAMILLES_T1}
        for a in annees_train_val:
            indic_a = get_indic_for_pres_year(a)
            sc_t1_a = scores_t1[a].reindex(depts_all)[FAMILLES_T1].fillna(0)
            X_val_parts.append(indic_a.values)
            for f in FAMILLES_T1:
                y_val_parts_t1[f].append(sc_t1_a[f].values)

        X_val_train = np.vstack(X_val_parts)
        sc_val = StandardScaler()
        X_val_tr_s = sc_val.fit_transform(X_val_train)

        # Indicateurs pour l'année test
        X_val_test  = get_indic_for_pres_year(annee_test).values
        X_val_te_s  = sc_val.transform(X_val_test)

        pred_val = {}
        mae_val  = {}
        r2_val   = {}
        for f in FAMILLES_T1:
            y_tr = np.concatenate(y_val_parts_t1[f])
            y_te = scores_t1[annee_test].reindex(depts_all)[f].fillna(0).values
            rf, gbr = _build_vote_model(X_val_tr_s, y_tr)
            p = _ensemble_predict(rf, gbr, X_val_te_s)
            pred_val[f] = p
            mae_val[f]  = mean_absolute_error(y_te, p)
            r2_val[f]   = r2_score(y_te, p)

        pred_val_norm = _normalize_scores(pred_val, FAMILLES_T1)

        vainq_pred = np.array(FAMILLES_T1)[
            np.stack([pred_val_norm[f] for f in FAMILLES_T1], axis=1).argmax(axis=1)
        ]
        vainq_reel = scores_t1[annee_test].reindex(depts_all)[FAMILLES_T1].idxmax(axis=1).values
        acc = np.mean(vainq_pred == vainq_reel)

        val_results = {
            "annee_test":  annee_test,
            "accuracy_vainqueur_dept": acc,
            "mae_par_famille":  mae_val,
            "r2_par_famille":   r2_val,
            "mae_moyen": np.mean(list(mae_val.values())),
        }
    results = {
        "t1_national":    score_nat_t1,
        "t1_par_dept":    df_res_t1,
        "t2_national":    score_nat_t2,
        "t2_par_dept":    df_res_t2,
        "validation":     val_results,
        "rf_importances": rf_importances,
        "features":       FEATURES,
    }

    logger.info(f"RandomForest_GradientBoosting: validation done (accuracy={acc:.4f}, mae_moyen={val_results['mae_moyen']:.4f})")

    export_csv(results, annee_cible=annee_cible, verbose=verbose)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# EXPORT CSV — 3 fichiers pour la datavisualisation
# ─────────────────────────────────────────────────────────────────────────────

def export_csv(results: dict, annee_cible: int = 2024,
               prefix: str = "RandomForest_GradientBoosting", verbose: bool = True):
    """
    Génère 3 fichiers CSV exploitables pour la dataviz :

    1. {prefix}_national.csv
       Une ligne par famille, avec scores T1, T2, métriques de validation
       et infos sur les modèles utilisés.

    2. {prefix}_par_dept.csv
       Une ligne par département, avec scores T1 et T2 par famille,
       vainqueur T1, vainqueur T2.

    3. {prefix}_importance_criteres.csv
       Importance de chaque critère socio-éco dans le Random Forest,
       par famille politique (T1).
    """

    # Export folder (project-relative)
    export_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, "[ia]exports"))
    os.makedirs(export_dir, exist_ok=True)

    t1   = results["t1_national"]
    t2   = results["t2_national"]
    val  = results["validation"]
    imp  = results.get("rf_importances", {})
    feat = results.get("features", [])

    familles_t1 = sorted(t1.keys(), key=lambda f: -t1[f])
    familles_t2 = sorted(t2.keys(), key=lambda f: -t2[f]) if t2 else []

    # ── 1. NATIONAL ──────────────────────────────────────────────────────────
    rows_nat = []
    for f in familles_t1:
        row = {
            "annee_cible":          annee_cible,
            "famille":              f,
            "tour":                 "T1",
            "score_pct":            round(t1[f], 2),
            "rang_t1":              familles_t1.index(f) + 1,
            "qualifie_t2":          f in familles_t2,
            "score_t2_pct":         round(t2.get(f, float("nan")), 2) if t2 else float("nan"),
            "rang_t2":              (familles_t2.index(f) + 1) if f in familles_t2 else float("nan"),
            # Validation
            "validation_annee_test":        val.get("annee_test", ""),
            "validation_accuracy_dept_pct": round(val.get("accuracy_vainqueur_dept", float("nan")) * 100, 1),
            "validation_mae_famille":       round(val.get("mae_par_famille", {}).get(f, float("nan")), 2),
            "validation_r2_famille":        round(val.get("r2_par_famille", {}).get(f, float("nan")), 3),
            "validation_mae_moyen":         round(val.get("mae_moyen", float("nan")), 2),
            # Modèles
            "modele_t1":            "RandomForest(n=300,depth=5) + GradientBoosting(n=200,lr=0.05) — poids 2:1",
            "modele_etape1":        "Ridge(alpha=1.0) par critère",
            "normalisation":        "softmax linéaire (sum=100%)",
        }
        rows_nat.append(row)

    df_nat = pd.DataFrame(rows_nat)
    path_nat = os.path.join(export_dir, f"{prefix}_national.csv")
    df_nat.to_csv(path_nat, index=False, sep=";")

    # ── 2. PAR DÉPARTEMENT ───────────────────────────────────────────────────
    df_t1 = results["t1_par_dept"].copy().reset_index(drop=True)
    df_t1.columns = [
        c if c in ("departement", "vainqueur") else c + "_t1"
        for c in df_t1.columns
    ]
    df_t1 = df_t1.rename(columns={"vainqueur": "vainqueur_t1"})
    df_t1["annee_cible"] = annee_cible

    df_t2 = results["t2_par_dept"]
    if not df_t2.empty:
        df_t2 = df_t2.copy().reset_index(drop=True)
        df_t2.columns = [
            c if c in ("departement", "vainqueur") else c + "_t2"
            for c in df_t2.columns
        ]
        df_t2 = df_t2.rename(columns={"vainqueur": "vainqueur_t2"})
        df_dept = df_t1.merge(df_t2, on="departement", how="left")
    else:
        df_dept = df_t1

    path_dept = os.path.join(export_dir, f"{prefix}_par_dept.csv")
    df_dept.to_csv(path_dept, index=False, sep=";")

    # ── 3. IMPORTANCE DES CRITÈRES ───────────────────────────────────────────
    # Deux formats dans un seul fichier :
    #   • Section "top10"    : les 10 critères les plus importants par famille
    #   • Section "croise"   : tableau croisé, une ligne par critère, une colonne par famille
    if imp and feat:
        familles_imp = sorted(imp.keys())

        # --- Tableau croisé complet ---
        df_croise = pd.DataFrame({"critere": feat})
        for f in familles_imp:
            df_croise[f"importance_{f}"] = [round(float(v), 6) for v in imp[f]]
        # Rang moyen toutes familles (pour trier)
        df_croise["importance_moyenne"] = df_croise[
            [f"importance_{f}" for f in familles_imp]
        ].mean(axis=1).round(6)
        df_croise = df_croise.sort_values("importance_moyenne", ascending=False).reset_index(drop=True)
        df_croise.insert(0, "rang_moyen", range(1, len(df_croise) + 1))

        # Export
        path_croise = os.path.join(export_dir, f"{prefix}_importance_croise.csv")
        df_croise.to_csv(path_croise, index=False, sep=";")
