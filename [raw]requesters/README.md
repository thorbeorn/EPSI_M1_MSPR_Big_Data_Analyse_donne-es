# README — Couche RAW (`[raw]requesters`)

## 1. Objectif de la couche RAW

Le dossier `[raw]requesters` constitue la **couche d’ingestion brute** de l’ETL.

Son rôle est de :

* Télécharger les données depuis des sources externes (API, fichiers distants)
* Charger les données **sans transformation métier**
* Les convertir en objets **`pandas.DataFrame`**
* Garantir la traçabilité via des logs
* Assurer la robustesse (gestion d’erreurs, fichiers temporaires, nettoyage)

Cette couche correspond à l’étape **Extract** de l’architecture ETL.

---

## 2. Principes de conception

Les modules RAW respectent les règles suivantes :

### Pas de transformation métier

* Aucune agrégation
* Aucun nettoyage métier
* Aucune logique analytique

### Responsabilités limitées

Chaque fonction :

* Télécharge une ressource
* Charge les données
* Retourne un DataFrame ou un dictionnaire de DataFrames

### Robustesse

* `try / except`
* `requests.raise_for_status()`
* Gestion des fichiers temporaires (compatible Windows)
* Suppression systématique des fichiers temporaires

### Traçabilité

Chaque module utilise le logging :

* `INFO` : étapes principales
* `DEBUG` : détails techniques
* `ERROR` : exceptions

---

## 3. Structure du dossier

```
[raw]/requesters/
│
├── melodi.py
├── parquet.py
├── xls.py
├── mixedxlsxzip.py
└── README.md
```

---

## 4. Modules disponibles

### 4.1 MELODI API

**Fonction**

```
creer_dataframe_depuis_melodi_api_url(url: str) -> pd.DataFrame
```

**Description**

* Interroge l’API MELODI
* Parse le JSON
* Extrait :

  * dimensions
  * attributes (si présents)
  * OBS_VALUE_NIVEAU (si présent)
* Retourne un DataFrame

**Cas gérés**

* Absence d’attributs
* Absence de valeur
* Observations vides
* JSON invalide

---

### 4.2 Fichiers Excel multiples

**Fonction**

```
creer_dataframe_depuis_multiple_url(urls: dict) -> dict
```

**Entrée**

```
{
    "2022": "https://.../file.zip",
    "2023": "https://.../file.xlsx"
}
```

**Description**

* Télécharge les fichiers
* Gère :

  * Excel direct
  * ZIP contenant plusieurs Excel
* Charge toutes les feuilles
* Concatène les feuilles de même nom

**Sortie**

```
{
    "2022": {
        "Sheet1": DataFrame,
        "Sheet2": DataFrame
    }
}
```

---

### 4.3 Parquet avec métadonnées

**Fonction**

```
creer_dataframe_depuis_parquet_url(url, metadata) -> pd.DataFrame
```

**Description**

* Télécharge un fichier Parquet
* Charge avec pandas
* Ajoute des métadonnées dans :

```
df.attrs["metadata"]
```

**Métadonnées acceptées**

* dictionnaire Python
* chemin vers fichier JSON

---

### 4.4 Excel simple

**Fonction**

```
creer_dataframe_depuis_xls_url(url, sheet_name) -> pd.DataFrame
```

**Description**

* Télécharge un fichier Excel
* Charge une feuille spécifique
* Retourne un DataFrame

---

## 5. Gestion technique commune

### Téléchargement

```
requests.get(..., verify=False)
```

(Désactivation SSL pour certaines sources publiques)

---

### Fichiers temporaires

Utilisation de :

```
tempfile.NamedTemporaryFile(delete=False)
```

Puis suppression :

```
os.remove(file)
```

Compatible Windows.

---

## 6. Tests unitaires

Chaque module possède des tests dans :

```
test_unitaire/
```

Les tests utilisent **pytest**

### Principes

* Mock des appels réseau (`requests.get`)
* Aucune dépendance externe
* Données simulées en mémoire
* Tests des cas :

  * nominal
  * erreurs HTTP
  * données manquantes
  * formats invalides
  * cas limites

### Import spécifique

Le dossier `[raw]` contenant des crochets, les modules sont importés dynamiquement via :

```
importlib.util.spec_from_file_location(...)
```

---

## 7. Logging

Chaque fonction suit le même pattern :

* `INFO`

  * début du traitement
  * fin du traitement

* `DEBUG`

  * téléchargement
  * parsing
  * lecture fichier
  * nettoyage

* `ERROR`

  * en cas d’exception

---

## 8. Exemple d’utilisation

```python
from [raw].requesters.melodi import creer_dataframe_depuis_melodi_api_url

url = "https://api.example.com/data"
df = creer_dataframe_depuis_melodi_api_url(url)

print(df.head())
```

---

## 9. Rôle dans l’architecture globale

```
Sources externes
      ↓
RAW (requesters)   ← CE DOSSIER
      ↓
Transformation / nettoyage
      ↓
Stockage / Analyse
```

La couche RAW garantit :

* reproductibilité
* traçabilité
* isolation des sources externes
* testabilité

---

## 10. Qualité du code

* Tests unitaires automatisés
* Couverture élevée (>90%)
* Logging complet
* Gestion d’erreurs
* Architecture modulaire