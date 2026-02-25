macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirment.txt
python main.py

Windows
python3 -m venv .venv
.venv\Scripts\Activate
pip install -r requirment.txt
python main.py

---

# RAW – Extraction des données MELODI

## Description

Dans le cadre de l’ETL, le module **RAW** est responsable de l’extraction des données depuis l’API MELODI et de leur mise à disposition sous forme exploitable.

La fonction `creer_dataframe_depuis_melodi_api_url` interroge une URL de l’API MELODI, parse la réponse JSON et la transforme en **DataFrame Pandas**.

Cette étape correspond à la phase **Extract** de l’architecture ETL.

---

## Fonction principale

### `creer_dataframe_depuis_melodi_api_url(melodi_url: str) -> pd.DataFrame`

#### Objectif

* Télécharger les données depuis l’API MELODI
* Extraire les observations
* Aplatir les dimensions, attributs et mesures
* Retourner un DataFrame structuré

#### Paramètre

| Nom        | Type | Description                  |
| ---------- | ---- | ---------------------------- |
| melodi_url | str  | URL de l’endpoint API MELODI |

#### Retour

* `pd.DataFrame` contenant :

  * les **dimensions**
  * les **attributs** (si présents)
  * la mesure `OBS_VALUE_NIVEAU`

---

## Fonctionnement

1. Appel HTTP via `requests`
2. Désérialisation du JSON
3. Lecture des champs principaux :

   * `title`
   * `identifier`
   * `observations`
4. Pour chaque observation :

   * extraction des dimensions
   * ajout des attributs (si disponibles)
   * récupération de la valeur de mesure (si présente)
5. Construction du DataFrame final

---

## Gestion des cas particuliers

La fonction gère les situations suivantes :

* Absence du champ `attributes`
* Absence de la clé `value` dans `OBS_VALUE_NIVEAU`
* Liste d’observations vide
* Erreur réseau ou JSON invalide (exception levée)

Des logs sont produits pour :

* le téléchargement
* les étapes d’extraction
* les erreurs éventuelles

---

## Exemple d’utilisation

```python
from raw.requesters.melodi import creer_dataframe_depuis_melodi_api_url

url = "https://api.melodi.fr/dataset"
df = creer_dataframe_depuis_melodi_api_url(url)

print(df.head())
```

---

## Tests unitaires

Les tests unitaires ont été implémentés avec **pytest**.

### Objectifs des tests

* Isoler les appels réseau via mock (`requests.get`)
* Vérifier la transformation en DataFrame
* Tester les cas limites

### Cas couverts

| Test               | Description                         |
| ------------------ | ----------------------------------- |
| Cas nominal        | Données complètes et valides        |
| Sans valeur        | `OBS_VALUE_NIVEAU` sans clé `value` |
| JSON invalide      | Réponse non parsable                |
| Observations vides | Retour d’un DataFrame vide          |

### Lancement

Depuis la racine du projet :

```bash
pytest test_unitaire/test_melodi.py
```

Couverture actuelle : **~96%**

```bash
pytest --cov="[raw]requesters"
```

---

## Position dans l’ETL

```
RAW (ce module)
    ↓
DataFrame brut
    ↓
Transform (nettoyage / normalisation)
    ↓
Load (base / stockage)
```

Ce module ne réalise **aucune transformation métier**, uniquement l’extraction et la structuration brute.

---

## Choix techniques pour ce module

* **Pandas** pour la manipulation tabulaire
* **Requests** pour l’appel API
* **Logging** pour la traçabilité
* Désactivation des warnings SSL pour certains environnements internes
* Import dynamique du module dans les tests (dossier `[raw]requesters`)