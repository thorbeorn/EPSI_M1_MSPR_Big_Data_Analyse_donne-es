# EPSI M1 MSPR – Analyse des Données (Big Data)

Ce projet porte sur la mise en place d'une chaîne de traitement de données (ETL) et d'un workflow d'apprentissage automatique visant à analyser, préparer et prédire des indicateurs liés à des données socio-politiques (par exemple élections présidentielles). Il est organisé en couches : **raw → silver → gold → IA**.

---

## 🧭 Vue d'ensemble du fonctionnement

### 1) Extraction (Raw)
- Les données sont récupérées depuis des sources diverses (fichiers CSV, XLS/XLSX, Parquet, API MELDI, etc.).
- Les scripts d'`[raw]requesters/` récupèrent ces données et les déposent dans une zone de stockage locale ou en mémoire.

### 2) Transformation / Nettoyage (Silver)
- Les données brutes sont nettoyées, homogénéisées et enrichies.
- `[silver]transformers/` contient des fonctions de nettoyage (normalisation des colonnes, correction typographiques, uniformisation des catégories, etc.).

### 3) Chargement / Stockage (Gold)
- Les données transformées sont stockées en **format Parquet** dans MinIO (bucket `gold`).
- Le dossier `[load]loaders/` contient les modules pour valider et charger ces jeux de données.

### 4) Modélisation & Prédictions (IA)
- Les scripts de `[ia]prediction/` entraînent plusieurs modèles (AdaBoost, Decision Tree, MLP, SVM, RandomForest/GradientBoosting).
- Les résultats (métriques, export CSV) sont stockés dans `[ia]exports/`.

---

## 🗂️ Arborescence principale (vue simplifiée)

```text
.
├── [docker]conf/              # Configuration Docker (MinIO, DB, etc.)
├── [raw]requesters/           # Extracteurs de données (CSV, XLS, Parquet, MELDI, ...)
├── [silver]transformers/      # Nettoyage / transformation (couche Silver)
├── [load]loaders/             # Chargement / validation / sauvegarde (Gold)
├── [gold]dashboards/          # Génération d'exports / dashboards
├── [ia]prediction/            # Scripts de modélisation (ML)
├── [ia]exports/               # Résultats d'entraînement / prédictions
├── [ETL]test_unitaire/        # Tests unitaires pour l'ETL
├── [IA]test_unitaire/         # Tests unitaires pour l'IA
├── etl.py                     # Pipeline ETL principal
├── ia.py                      # Pipeline IA principal
├── requirement.txt            # Dépendances Python
├── README.md                  # Documentation (ce fichier)
└── docs-*/                   # Documentation et schémas supplémentaires
```

---

## 📈 Schéma de fonctionnement (workflow)

1) **Démarrer l’infrastructure** (MinIO + base de données)
2) **Lancer le pipeline ETL (etl.py)**
   - Extraction via `[raw]requesters/`
   - Nettoyage via `[silver]transformers/`
   - Chargement dans MinIO via `[load]loaders/`
3) **Lancer le pipeline IA (ia.py)**
   - Chargement des données Gold depuis MinIO (`[ia]prediction/load.py`)
   - Entraînement des modèles et génération d’exports dans `[ia]exports/`

---

## 🛠️ Mise en route (Quick start)

### 1) Démarrer l’infrastructure Docker (MinIO + PostgreSQL)

**macOS / Linux**
```bash
cd "[docker]conf"
docker-compose up -d
cd ..
```

**Windows (PowerShell)**
```powershell
cd "[docker]conf"
docker-compose up -d
cd ..
```

### 2) Installer les dépendances Python

```bash
python3 -m venv .venv
# macOS / Linux
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate

pip install -r requirement.txt
```

### 3) Exécuter les pipelines

```bash
python etl.py   # ETL : extraction, transformation, chargement
python ia.py    # IA : entraînement + prédictions
```

---

## 🧪 Tests

```bash
pytest test_unitaire/test_cleaning_functions.py
python -m pytest --cov="[silver]transformers"
python -m pytest --cov="[silver]transformers" --cov-report=term-missing
```

---

## 👀 Où sont les données ?

- **MinIO** (S3 compatible) est utilisé pour stocker les données Gold.
- Le bucket ciblé est nommé : `gold`
- Le module principal utilisé pour charger ces données est `[ia]prediction/load.py`.

---

## 📌 Notes / Conseils

- Si vous modifiez les schémas de données (colonnes, types), vérifiez que les transformations Silver et les modèles IA restent cohérents.
- Il est recommandé de versionner les exports dans `[ia]exports/` si vous souhaitez comparer les résultats entre plusieurs runs.
- Pour ajouter un nouveau modèle, créez un nouveau script dans `[ia]prediction/` et mettez-le à jour dans `ia.py`.

---

## 🗃️ Documentation & schémas additionnels

- `docs-etl/` contient des diagrammes, des données de qualité, et la documentation du workflow ETL.
- `docs-ia/` contient des exports et schémas relatifs à la partie IA.

---

## 📄 Fichiers existants

Ce README est la documentation globale du projet. Les dossiers contiennent parfois leurs propres README locaux (par exemple `[load]loaders/README.md`, `[ia]prediction/README.md`, etc.) pour les détails spécifiques.
