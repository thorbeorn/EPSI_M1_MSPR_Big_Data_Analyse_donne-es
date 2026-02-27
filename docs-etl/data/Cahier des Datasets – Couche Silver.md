# Cahier des Datasets – Couche Silver

**Niveau** : Silver  
**Grain principal** : (Code_departement, annee)  
**Objectif** : Fournir des jeux de données nettoyés, normalisés et prêts pour analyse ou modélisation.

---

# 1. Architecture des données

## Pipeline

Raw sources :
- API INSEE (Melodi)
- fichiers XLS / XLSX
- archives ZIP
- fichiers Parquet Data.gouv

Transformation :
- nettoyage des formats
- normalisation des codes département
- conversion numérique
- agrégation annuelle
- enrichissement via métadonnées JSON

---

# 2. Clé de jointure standard

| Colonne | Type | Description |
|---|---|---|
| Code_departement | string | Code officiel (01–95, 2A, 2B, 971–988) |
| annee | int | Année d’observation |

Toutes les tables Silver doivent être joignables sur cette clé (sauf dimensions spécifiques).

---

# 3. Catalogue des datasets Silver

---

## 3.1 silver_delinquance_df

**Source** : Ministère de l’Intérieur (Data.gouv)  
**Période** : selon disponibilité

### Description
Statistiques de délinquance agrégées par département et année.

### Variables principales

| Colonne | Type | Description |
|---|---|---|
| Code_departement | string | Département |
| annee | int | Année |
| nombre_faits | float | Nombre total de faits |
| taux_pour_mille | float | Taux pour 1000 habitants |

---

## 3.2 silver_taux_chommage_df

**Source** : INSEE  
**Période** : trimestrielle agrégée en annuel

### Description
Taux de chômage moyen annuel par département.

| Colonne | Type | Description |
|---|---|---|
| Code_departement | string |
| annee | int |
| taux_chomage | float | Pourcentage (0–100) |

---

## 3.3 silver_age_moyen_df

**Source** : INSEE Melodi

### Description
Âge moyen de la population par département.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| age_moyen | float |

---

## 3.4 silver_population_active_df

**Source** : INSEE Melodi  
**Métadonnées** : population_active.json

### Description
Répartition de la population active par statut d’emploi.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| statut_emploi | string |
| population | float |

---

## 3.5 silver_categorie_professionnelle_df

**Source** : INSEE  
**Métadonnées** : categorie_professionnelle.json

### Description
Distribution par catégories socio-professionnelles.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| categorie_professionnelle | string |
| effectif | float |

---

## 3.6 silver_equipement_sportif_df

**Source** : Ministère des Sports

### Description
Stock d’équipements sportifs par département.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| nb_equipements | float |

---

## 3.7 silver_revenu_moyen_df

**Source** : DGFiP  
**Période** : 1984–2023

### Description
Revenu moyen fiscal par département.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| revenu_moyen | float |

---

## 3.8 silver_etablissement_culturel_df

**Source** : Ministère de la Culture

### Description
Nombre d’établissements culturels.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| nb_etablissements_culturels | float |

---

## 3.9 silver_pouvoir_achat_df

**Source** : INSEE

### Description
Indice de pouvoir d’achat par département.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| indice_pouvoir_achat | float |

---

## 3.10 silver_niveau_etude_df

**Source** : INSEE Melodi  
**Métadonnées** : niveau_etude.json

### Description
Répartition par niveau de diplôme.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| niveau_etude | string |
| population | float |

---

## 3.11 silver_abstention_votant_df

**Source** : Ministère de l’Intérieur

### Description
Participation électorale.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| tour | int |
| inscrits | float |
| votants | float |
| abstentions | float |

---

## 3.12 silver_president_sortant_df

**Source** : Résultats élections présidentielles  
**Métadonnées** : bords_politiques.json

### Description
Résultats du candidat sortant et famille politique.

| Colonne | Type |
|---|---|
| Code_departement | string |
| annee | int |
| tour | int |
| candidat | string |
| famille_politique | string |
| voix | float |
| pourcentage | float |

---

# 4. Normalisations communes

## Code département
- format string
- corrections appliquées (750 → 75, etc.)

## Année
- type int
- certaines tables décalées de -1 pour cohérence explicative

## Numériques
- conversion automatique
- suppression séparateurs milliers
- gestion virgule décimale

---

# 5. Métadonnées utilisées

| Fichier | Usage |
|---|---|
| metadata_delinquance.json | mapping infractions |
| bords_politiques.json | mapping candidats → famille |
| population_active.json | mapping statuts |
| categorie_professionnelle.json | mapping PCS |
| niveau_etude.json | mapping diplômes |

---

# 6. Qualité attendue

| Règle | Description |
|---|---|
| Pas de doublon | par clé principale |
| Valeurs ≥ 0 | pour indicateurs volumétriques |
| Codes valides | départements officiels |
| Couverture | ≥ 95 départements |

---
# 7. Usage cible

- Feature engineering (Gold)
- Analyse socio-économique territoriale
- Modélisation électorale
- Table unique fusionnée par département/année
