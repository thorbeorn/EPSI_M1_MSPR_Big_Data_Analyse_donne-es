# [ia]prediction

Ce dossier contient les scripts de **modélisation prédictive** (machine learning) pour le projet.

## 🧩 Contenu principal

- `load.py` — utilitaire pour charger des fichiers **Parquet** depuis MinIO (bucket `gold`) et les convertir en `pandas.DataFrame`.
- `data_quality.py` — fonctions/utilitaires pour évaluer la qualité des données (missing values, incohérences, etc.).

### Modèles implémentés

- `adaboost.py` — entraînement et évaluation d'un modèle AdaBoost.
- `decision_tree.py` — entraînement et évaluation d'un Decision Tree.
- `mlp.py` — entraînement et évaluation d'un réseau de neurones (MLP).
- `RandomForest_GradientBoosting.py` — entraînement et évaluation combinée Random Forest + Gradient Boosting.
- `svm.py` — entraînement et évaluation d'un Support Vector Machine.

## ✅ Utilisation générale

1. Assurez-vous que MinIO est en cours d'exécution et que le bucket `gold` contient les fichiers attendus.
2. Lancez le pipeline principal :
   ```bash
   python ia.py
   ```
3. Les résultats d'entraînement/évaluation sont publiés dans `[ia]exports/`.

## 🔎 Notes

- Les scripts d'entraînement utilisent `pandas`, `scikit-learn`, et incluent des étapes de prétraitement (imputation, normalisation, encodage).
- Les données de référence sont fusionnées entre les indicateurs et le résultat attendu (ex: famille politique) pour l'entraînement.
