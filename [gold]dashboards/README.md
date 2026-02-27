# 📊 MSPR – Data Engineering Pipeline (MySQL → Pandas → MinIO)

Ce projet permet de :

* Extraire des données depuis une base **MySQL**
* Construire des tables **Gold** consolidées via des requêtes SQL
* Exporter les résultats en **CSV** et **Parquet**
* Uploader automatiquement les fichiers vers **MinIO** (Data Lake S3 compatible)

---

## 🏗️ Architecture

```
MySQL (mspr-db)
        ↓
   Pandas (SQLAlchemy)
        ↓
 Transformation / Agrégation
        ↓
 Export CSV / Parquet (in-memory)
        ↓
 MinIO (Bucket: gold)
```

---

## ⚙️ Technologies utilisées

* Python 3.x
* pandas
* SQLAlchemy
* PyMySQL
* MinIO Python SDK
* PyArrow (pour Parquet)
* MySQL

---

## 🔌 Connexion base de données

Connexion via SQLAlchemy :

```python
engine = create_engine(
    "mysql+pymysql://USER:PASSWORD@localhost:3306/mspr-db"
)
```

---

## ☁️ Configuration MinIO

Connexion au serveur MinIO :

* Endpoint : `localhost:9000`
* Access Key : `mspr-admin`
* Secret Key : `********`
* Buckets utilisés :

  * `gold`
  * `data-lake`

---

## 📂 Fonctions principales

### 1️⃣ `upload_df_to_minio()`

Permet d’uploader un `DataFrame` Pandas :

* Format supporté :

  * `csv`
  * `parquet`
* Sans création de fichier local (utilise `BytesIO`)
* Création automatique du bucket si inexistant

Exemple :

```python
upload_df_to_minio(
    df,
    file_format="csv",
    bucket_name="gold",
    object_name="all_indicator.csv"
)
```

---

### 2️⃣ `create_gold_all_indicator_df()`

Construit une table **Gold consolidée** regroupant :

* 👥 Age moyen
* 🚔 Délinquance
* 💰 Revenu moyen
* 📉 Taux de chômage
* 🏟️ Équipements sportifs
* 🎭 Établissements culturels
* 🎓 Niveau d’étude
* 👷 Population active
* 🏢 Catégorie professionnelle
* 🛒 Pouvoir d’achat
* 🗳️ Abstention / votes blancs / nuls

Puis exporte :

```
gold/all_indicator.csv
gold/all_indicator.parquet
```

---

### 3️⃣ `create_gold_all_president_df()`

Construit une table contenant :

* Le candidat ayant obtenu le **maximum de voix au second tour**
* Par département et par année

Puis exporte :

```
gold/all_president.csv
gold/all_president.parquet
```

---

## ▶️ Exécution

Il suffit d’appeler :

```python
create_gold_all_indicator_df()
create_gold_all_president_df()
```

---

## 📦 Structure des données

Les fichiers générés sont stockés dans MinIO :

```
gold/
 ├── all_indicator.csv
 ├── all_indicator.parquet
 ├── all_president.csv
 └── all_president.parquet
```