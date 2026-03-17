import logging

import pandas as pd
import numpy as np
from sklearn.neural_network import MLPClassifier
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, mean_absolute_error, r2_score
from collections import Counter

# CONFIGURATION DU LOGGING
logger = logging.getLogger(__name__)


def train_logistic_model(df_indicateurs, df_resultats):
    """
    Entraîne un réseau de neurones MLP (Multi-Layer Perceptron).
    Architecture : 3 couches cachées (128, 64, 32).
    Retourne : (accuracy, report, resultats_2024, mae, r2)
    """

    logger.info("MLP: début de l'entraînement")

    if 'code_departement' in df_resultats.columns:
        df_resultats = df_resultats.rename(columns={'code_departement': 'Code_departement'})
        logger.debug("MLP: renommage de la colonne code_departement")

    df_train_full = pd.merge(df_indicateurs, df_resultats, on=['annee', 'Code_departement'], how='inner')
    df_2024 = df_indicateurs[df_indicateurs['annee'] == 2024].copy()

    y_train_full = df_train_full['[president_sortant]famille_politique']
    le = LabelEncoder()
    y_encoded = le.fit_transform(y_train_full)

    cols_to_drop = ['annee', 'Code_departement', '[president_sortant]tour',
                    '[president_sortant]famille_politique', '[president_sortant]pourcentage']
    X_train_full = df_train_full.drop(columns=[c for c in cols_to_drop if c in df_train_full.columns])
    X_train_full = X_train_full.select_dtypes(include=[np.number])
    X_train_full = X_train_full.replace([np.inf, -np.inf], np.nan)

    if not df_2024.empty:
        df_2024 = df_2024.replace([np.inf, -np.inf], np.nan)

    features = X_train_full.columns

    imputer = SimpleImputer(strategy='median')
    scaler  = StandardScaler()

    X_imputed = imputer.fit_transform(X_train_full)
    X_scaled  = scaler.fit_transform(X_imputed)

    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
    )
    logger.debug(f"MLP: train/test split, train={X_train.shape}, test={X_test.shape}")

    model = MLPClassifier(
        hidden_layer_sizes=(128, 64, 32),
        activation='relu',
        solver='adam',
        max_iter=1000,
        random_state=42,
        early_stopping=True,
        validation_fraction=0.1
    )
    model.fit(X_train, y_train)
    logger.info("MLP: entraînement terminé")

    y_pred   = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    report   = classification_report(y_test, y_pred, target_names=le.classes_, zero_division=0)
    logger.info(f"MLP: évaluation terminée - accuracy={accuracy:.4f}, mae={mae:.4f}, r2={r2:.4f}")

    model.fit(X_scaled, y_encoded)
    logger.info("MLP: ré-entraînement sur l'ensemble des données terminé")

    if not df_2024.empty:
        X_2024     = df_2024[features]
        X_2024_imp = imputer.transform(X_2024)
        X_2024_sc  = scaler.transform(X_2024_imp)
        preds_2024 = le.inverse_transform(model.predict(X_2024_sc))
        resultats_2024 = dict(Counter(preds_2024))
    else:
        resultats_2024 = {"Erreur": "Aucune donnée trouvée pour 2024"}

    return accuracy, report, resultats_2024, mae, r2
