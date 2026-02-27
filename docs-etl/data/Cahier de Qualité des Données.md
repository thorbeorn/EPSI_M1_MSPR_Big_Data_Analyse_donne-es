# Cahier de Qualité des Données  

**Version** : 1.0  
**Objectif** : Garantir la fiabilité, la cohérence et la traçabilité des données intégrées dans le pipeline analytique.

---

# 1. Contexte et périmètre

Ce document définit les règles de qualité appliquées aux données socio-économiques et électorales intégrées dans le pipeline.

## Sources principales

- INSEE (population, chômage, diplômes, PCS)
- DGFiP (revenus fiscaux)
- Ministère de l’Intérieur (élections, abstention)
- Ministère des Sports (équipements)
- Ministère de la Culture (établissements)
- Autres sources publiques nationales

## Grain d’analyse

La majorité des jeux de données sont normalisés au niveau :

```
(Code_departement, annee)
```

Certaines tables incluent une dimension supplémentaire :
- tour électoral
- statut d’emploi
- tranche d’âge
- catégorie socio-professionnelle

---

# 2. Dimensions de qualité

Le contrôle qualité s’appuie sur 6 dimensions.

## 2.1 Complétude

Objectif : éviter les valeurs manquantes critiques.

### Règles générales

- `Code_departement` : obligatoire
- `annee` : obligatoire
- Colonnes indicateurs : tolérance aux NaN uniquement si explicitement justifié

### Contrôles

| Champ | Règle |
|---|---|
| Code_departement | non null |
| annee | non null |
| Valeurs numériques | NaN < 5% (sauf données historiques ou nationales) |

---

## 2.2 Validité

Objectif : vérifier la conformité des valeurs au domaine attendu.

### Code département

Formats autorisés :
- `01` à `95`
- `2A`, `2B`
- `971` à `988`

Corrections appliquées :
- `750` → `75`
- `2A0` → `2A`
- Codes DOM normalisés (ZA → 971, etc.)

### Année

- Type : entier
- Intervalle attendu : `1950 ≤ annee ≤ année courante`

### Valeurs numériques

- Pas de valeurs négatives pour :
  - populations
  - équipements
  - inscrits
  - revenus
- Taux compris dans :
  - chômage : 0–100
  - abstention : 0–100

---

## 2.3 Cohérence

Objectif : assurer la logique interne des données.

### Élections

- `abstentions ≤ inscrits`
- `blancs + nuls ≤ votants`
- Année décalée de -1 pour variables explicatives

### Revenus

- Revenu moyen > 0
- Unité homogène (euros)

### Équipements sportifs

- Stock calculé par cumul de deltas
- Stock non décroissant sauf sortie d’équipement

---

## 2.4 Unicité

Objectif : éviter les doublons analytiques.

### Clé primaire standard

```
(Code_departement, annee)
```

Variantes :

| Dataset | Clé |
|---|---|
| Population active | dept + annee + Statut_emploi |
| Abstention | dept + annee + tour |
| Président sortant | dept + annee + tour + candidat |

Contrôle :

```python
df.duplicated(subset=key_columns).sum() == 0
```

---

## 2.5 Intégrité temporelle

Objectif : cohérence entre sources.

### Décalages temporels

Certaines sources sont alignées sur **année N-1** :

| Dataset           | Décalage |
| ----------------- | -------- |
| Président sortant | -1       |
| Abstention        | -1       |
| Population active | -1       |
| Niveau d’étude    | -1       |

Règle :

> Les variables explicatives doivent correspondre à l’état avant l’élection.

---

## 2.6 Traçabilité

Chaque transformation doit être :

* reproductible
* documentée
* loggée

### Logging

* erreurs critiques → `logger.error`
* anomalies non bloquantes → `logger.warning`

---

# 3. Contrôles par dataset

---

## 3.1 Délinquance

**Agrégation** : somme des faits, moyenne des taux.

Contrôles :

* nombre ≥ 0
* taux_pour_mille ≥ 0
* présence de toutes les années disponibles

---

## 3.2 Taux de chômage

Contrôles :

* 0 ≤ taux ≤ 100
* 4 trimestres minimum avant agrégation
* moyenne annuelle calculée

---

## 3.3 Population active

Contrôles :

* valeurs ≥ 0
* somme des tranches cohérente avec total national (contrôle macro)

---

## 3.4 Revenus

Période : 1984–2023

Points sensibles :

* formats Excel hétérogènes
* unités en milliers pour 2021+

Contrôles :

* revenu moyen > 0
* continuité temporelle
* exclusion du code `B31`

---

## 3.5 Équipements sportifs

Contrôles :

* année mise en service ≤ année fin
* stock cumulatif ≥ 0
* filtre : année ≥ 1950

---

## 3.6 Élections

Président sortant :

* famille politique non nulle (si mapping existant)

Abstention :

* inscrits ≥ abstentions
* valeurs agrégées par département

---

## 3.7 Niveau d’étude

Contrôles :

* valeurs ≥ 0
* absence de doublons après pivot
* agrégats supprimés

---

# 4. Normalisation

## 4.1 Texte

Fonction utilisée :

```
normaliser()
```

Actions :

* minuscules
* suppression accents
* comparaisons robustes

---

## 4.2 Numériques

Fonction :

```
_parse_numeric_col()
```

Gère :

* espaces milliers
* virgules décimales
* conversion float

---

# 5. Gestion des anomalies

| Type                      | Action      |
| ------------------------- | ----------- |
| Valeur non convertible    | NaN         |
| Code département invalide | log warning |
| Colonnes manquantes       | exception   |
| Format inattendu          | exception   |

Principe :

> Fail fast sur structure, tolérance sur contenu.

---

# 6. Indicateurs de qualité (KPI)

À calculer après chaque ingestion :

| KPI                     | Seuil    |
| ----------------------- | -------- |
| Taux de complétude      | > 99%    |
| Doublons                | 0        |
| Valeurs hors domaine    | 0        |
| Couverture départements | ≥ 95     |
| Couverture années       | continue |

---

# 7. Tests automatisés recommandés

### Exemple PyTest

```python
def test_no_duplicates(df):
    assert df.duplicated(["Code_departement", "annee"]).sum() == 0

def test_no_negative_values(df):
    numeric_cols = df.select_dtypes("number")
    assert (numeric_cols >= 0).all().all()
```

---

# 8. Gouvernance des données

## Versioning

* version par source
* historique des fichiers bruts

## Reproductibilité

* pipeline déterministe
* transformations idempotentes

## Documentation

* docstring par fonction
* dictionnaire de données
* ce cahier qualité

---

# 9. Risques identifiés

| Risque                              | Impact              | Mitigation             |
| ----------------------------------- | ------------------- | ---------------------- |
| Changement format Excel             | pipeline cassé      | clean_excel_block      |
| Code département incohérent         | jointure impossible | fix_departement        |
| Mapping JSON incomplet              | valeurs nulles      | contrôle de complétude |
| Données manquantes certaines années | biais modèle        | alerte KPI             |

---

# 10. Évolutions prévues

* Dashboard de monitoring qualité
* Tests Great Expectations
* Data contracts par source
* Alertes automatiques (email / Slack)
* Score qualité global par dataset

---

# Annexe : Clé standard du modèle

```
Code_departement : string
annee            : int
```

Toutes les tables doivent pouvoir être jointes sur cette clé.