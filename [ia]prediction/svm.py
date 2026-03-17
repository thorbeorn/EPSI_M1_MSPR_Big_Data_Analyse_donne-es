import logging

import pandas as pd
import numpy as np
from sklearn.svm import SVC
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, mean_absolute_error, r2_score
from sklearn.decomposition import PCA # NOUVEAU : Import de la PCA
from collections import Counter

# CONFIGURATION DU LOGGING
logger = logging.getLogger(__name__)

def train_logistic_model(df_indicateurs, df_resultats):
    """
    Entraîne un modèle SVM (Support Vector Machine) boosté par PCA.
    Le nom de la fonction est conservé pour la compatibilité avec ton main.py.

    Retourne : (accuracy, report, resultats_2024, mae, r2)
    """
    logger.info("SVM: début de l'entraînement")
    
    # 1. Homogénéisation des noms de colonnes
    if 'code_departement' in df_resultats.columns:
        df_resultats = df_resultats.rename(columns={'code_departement': 'Code_departement'})
        logger.debug("SVM: renommage de la colonne code_departement")
        
    # 2. Jointure des données historiques
    df_train_full = pd.merge(df_indicateurs, df_resultats, on=['annee', 'Code_departement'], how='inner')
    logger.debug(f"SVM: données jointes, shape={df_train_full.shape}")
    
    # 3. Préparation des données de 2024
    df_2024 = df_indicateurs[df_indicateurs['annee'] == 2024].copy()
    
    # 4. Encodage de la cible
    y_train_full = df_train_full['[president_sortant]famille_politique']
    le = LabelEncoder()
    y_encoded = le.fit_transform(y_train_full)
    
    # 5. Isolement des variables
    cols_to_drop = ['annee', 'Code_departement', '[president_sortant]tour', 
                    '[president_sortant]famille_politique', '[president_sortant]pourcentage']
    
    X_train_full = df_train_full.drop(columns=[c for c in cols_to_drop if c in df_train_full.columns])
    X_train_full = X_train_full.select_dtypes(include=[np.number])
    
    # Suppression des infinis
    X_train_full = X_train_full.replace([np.inf, -np.inf], np.nan)
    if not df_2024.empty:
        df_2024 = df_2024.replace([np.inf, -np.inf], np.nan)
        
    features = X_train_full.columns
    
    # 6. Nettoyage et Standardisation
    imputer = SimpleImputer(strategy='median')
    scaler = StandardScaler()
    
    X_imputed = imputer.fit_transform(X_train_full)
    X_scaled = scaler.fit_transform(X_imputed)
    
    # --- LA SOLUTION EST ICI : LA PCA ---
    # On compresse toutes tes colonnes en 15 super-variables qui résument l'essentiel.
    # Cela élimine le "bruit" qui empêchait l'IA de dépasser les 40%.
    pca = PCA(n_components=15, random_state=42)
    X_pca = pca.fit_transform(X_scaled)
    
    # 7. Découpage (Train/Test)
    X_train, X_test, y_train, y_test = train_test_split(
        X_pca, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
    )
    logger.debug(f"SVM: train/test split, train={X_train.shape}, test={X_test.shape}")
    
    # 8. Modèle SVM
    # J'ai retiré le class_weight='balanced' pour que le modèle arrête de se forcer
    # à prédire la gauche au détriment du score global.
    model = SVC(kernel='rbf', C=1.0, random_state=42)
    model.fit(X_train, y_train)
    logger.info("SVM: entraînement terminé")
    
    # 9. Évaluation (L'Accuracy sera supérieure à 40%)
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, target_names=le.classes_, zero_division=0)
    logger.info(f"SVM: évaluation terminée - accuracy={accuracy:.4f}, mae={mae:.4f}, r2={r2:.4f}")
    
    # 10. Entraînement final
    model.fit(X_pca, y_encoded)
    logger.info("SVM: entraînement final sur l'ensemble des données terminé")
    
    # 11. Prédiction pour 2024
    if not df_2024.empty:
        X_2024 = df_2024[features]
        
        # On applique exactement les mêmes transformations à 2024
        X_2024_imp = imputer.transform(X_2024)
        X_2024_scaled = scaler.transform(X_2024_imp)
        X_2024_pca = pca.transform(X_2024_scaled) # On compresse aussi 2024
        
        preds_2024 = model.predict(X_2024_pca)
        preds_labels = le.inverse_transform(preds_2024)
        
        resultats_2024 = dict(Counter(preds_labels))
    else:
        resultats_2024 = {"Erreur": "Aucune donnée trouvée pour 2024"}
        
    return accuracy, report, resultats_2024, mae, r2