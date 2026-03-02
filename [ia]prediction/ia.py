import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.compose import ColumnTransformer


def train_logistic_model(df_indicateurs, df_resultats):
    
    # =========================
    # 1. Harmonisation colonnes
    # =========================
    df_indicateurs = df_indicateurs.copy()
    df_resultats = df_resultats.copy()

    df_indicateurs.columns = df_indicateurs.columns.str.strip()
    df_resultats.columns = df_resultats.columns.str.strip()

    # Uniformiser nom colonne département
    if "Code_departement" in df_indicateurs.columns:
        df_indicateurs = df_indicateurs.rename(columns={"Code_departement": "code_departement"})

    # =========================
    # 2. Merge
    # =========================
    df = df_indicateurs.merge(
        df_resultats,
        on=["annee", "code_departement"],
        how="inner"
    )

    # =========================
    # 3. Feature Engineering
    # =========================

    # Taux abstention
    if "[abstention_votant]inscrits" in df.columns:
        df["taux_abstention"] = (
            df["[abstention_votant]abstentions"] /
            df["[abstention_votant]inscrits"]
        )

        df["taux_blanc"] = (
            df["[abstention_votant]blancs"] /
            df["[abstention_votant]inscrits"]
        )

    # Remplacer inf/nan
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna()

    # =========================
    # 4. Sélection features
    # =========================
    target_col = "[president_sortant]famille_politique"

    features = df.drop(columns=[
        target_col,
        "annee",
        "code_departement"
    ])

    X = features
    y = df[target_col]

    # =========================
    # 5. Encodage target
    # =========================
    le = LabelEncoder()
    y_encoded = le.fit_transform(y)

    # =========================
    # 6. Split temporel
    # =========================
    train_mask = df["annee"] == 2017
    test_mask  = df["annee"] == 2022

    X_train = X[train_mask]
    y_train = y_encoded[train_mask]

    X_test = X[test_mask]
    y_test = y_encoded[test_mask]

    # =========================
    # 7. Pipeline modèle
    # =========================
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(
            penalty="l2",
            C=1.0,
            max_iter=2000,
            class_weight="balanced"
        ))
    ])

    # =========================
    # 8. Entraînement
    # =========================
    pipeline.fit(X_train, y_train)

    # =========================
    # 9. Évaluation
    # =========================
    y_pred = pipeline.predict(X_test)

    print("\n===== Classification Report (Test 2022) =====\n")
    print(classification_report(y_test, y_pred, target_names=le.classes_))

    print("\n===== Confusion Matrix =====\n")
    print(confusion_matrix(y_test, y_pred))

    return pipeline, le