# [ia]exports

Ce dossier contient les **résultats d'exécution** des modèles d'apprentissage automatique présents dans `[ia]prediction/`.

## 📦 Contenu actuel

- `adaboost_result.json` — résultat de l'entraînement/évaluation du modèle AdaBoost
- `decision_tree_result.json` — résultat du modèle Decision Tree
- `mlp_result.json` — résultat du modèle MLP
- `svm_result.json` — résultat du modèle SVM
- `RandomForest_GradientBoosting_result.json` — résultat du modèle Random Forest + Gradient Boosting
- `RandomForest_GradientBoosting_national.csv` — export des prédictions agrégées au niveau national
- `RandomForest_GradientBoosting_par_dept.csv` — export des prédictions par département
- `RandomForest_GradientBoosting_importance_croise.csv` — importance croisée des features pour Random Forest / Gradient Boosting

## 🧠 À quoi servent ces fichiers ?

- Les fichiers JSON contiennent généralement des metrics (accuracy, MAE, R², classification report) et parfois des prédictions.
- Les exports CSV peuvent être utilisés pour des rapports, visualisations ou analyses complémentaires.

## 🔁 Mise à jour

Ces fichiers sont régénérés lorsque vous lancez `python ia.py` (ou les scripts individuels dans `[ia]prediction/`).
