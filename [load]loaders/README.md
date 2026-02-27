# 📊 Data Quality & Silver Data Pipeline

Ce projet implémente un pipeline de traitement **Silver Layer** avec :

* ✅ Audit qualité des DataFrames
* ☁️ Export des rapports JSON vers MinIO
* 🗄️ Sauvegarde automatique en base MySQL
* 📈 Construction automatique d’une table `indicateurs`

---

## 🏗️ Architecture du projet

```
[load]loaders
- quality.py
- save.py
```

### 📂 `quality.py`

Contient :

* `audit_dataframe(df, df_name)`
  → Analyse qualité d’un DataFrame :

  * Nombre de lignes / colonnes
  * Doublons
  * Valeurs manquantes (%)
  * Valeurs négatives (colonnes numériques)
  * Score qualité global

* `audit_all_silver_dataframes(namespace)`
  → Audite tous les DataFrames dont le nom commence par `silver_`
  → Génère un rapport consolidé
  → Envoie le JSON dans **MinIO**

* `upload_json_to_minio()`
  → Upload direct en mémoire (sans fichier local)

---

### 📂 `save.py`

Contient :

* `save_all_silver_dataframes(dfs)`
  → Sauvegarde tous les DataFrames `silver_*` dans MySQL
  → Supprime automatiquement :

  * le préfixe `silver_`
  * le suffixe `_df`
    → Exemple :

  ```
  silver_clients_df → table SQL : clients
  ```

* `build_indicateurs_table(dfs)`
  → Construit une table `indicateurs` contenant toutes les combinaisons uniques :

  ```
  (Code_departement, annee)
  ```

  trouvées dans les tables Silver.

---

## ⚙️ Configuration requise

### 🐳 MinIO

Configuration actuelle :

* Endpoint : `localhost:9000`
* Bucket : `data-quality`
* Secure : `False` (HTTP local)

Dépendance :

```bash
pip install minio
```

---

### 🗄️ MySQL

Connexion utilisée :

```python
mysql+pymysql://mspr-user:PASSWORD@localhost:3306/mspr-db
```

Dépendances :

```bash
pip install sqlalchemy pymysql
```

---

### 📦 Autres dépendances

```bash
pip install pandas
```

---

## 🚀 Utilisation

### 1️⃣ Sauvegarder toutes les tables Silver

```python
save_all_silver_dataframes(globals())
```

Effet :

* Sauvegarde toutes les tables `silver_*`
* Génère automatiquement la table `indicateurs`

---

### 2️⃣ Lancer un audit qualité global

```python
audit_all_silver_dataframes(globals())
```

Effet :

* Audit de toutes les tables Silver
* Génération d’un JSON consolidé
* Upload dans MinIO

---

## 📊 Score Qualité

Le score est calculé ainsi :

```
100
- moyenne % valeurs manquantes
- pénalité doublons (%)
```

Score minimum : `0`
Score maximum : `100`

---

## 🧠 Convention obligatoire

Les DataFrames doivent :

* Commencer par `silver_`
* Être des objets `pandas.DataFrame`

Exemple valide :

```python
silver_clients_df = pd.DataFrame(...)
```

---

## 🔐 Sécurité (Important)

⚠️ Les identifiants sont actuellement en dur dans le code.
En production, utiliser :

* Variables d’environnement
* Fichier `.env`
* Vault / Secret Manager

---

## 📌 Résumé du pipeline

```
DataFrames Silver
        │
        ├── Audit qualité → JSON → MinIO
        │
        └── Sauvegarde → MySQL
                    │
                    └── Construction table indicateurs
```