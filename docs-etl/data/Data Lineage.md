# Data Lineage – Pipeline socio-économique et électoral

## Objectif

Documenter le flux de transformation des données depuis les sources externes (Raw) jusqu’aux datasets nettoyés (Silver), afin d’assurer :
- traçabilité
- reproductibilité
- auditabilité
- compréhension des dépendances

---

# 1. Vue d’ensemble de l’architecture

```

Sources externes
↓
Raw Layer (requesters)
↓
Silver Layer (clean_xxx)
↓
Stockage Silver (Parquet)
↓
(Optionnel) Gold Layer – Dataset fusionné

```

---

# 2. Raw Layer – Acquisition

Les données sont récupérées via des modules spécialisés :

| Module | Type de source |
|---|---|
| parquet | fichiers Parquet distants |
| xls | fichiers Excel |
| mixedxlsxzip | archives ZIP contenant Excel |
| melodi | API INSEE Melodi |

Fonctions principales :

- `creer_dataframe_depuis_parquet_url()`
- `creer_dataframe_depuis_xls_url()`
- `creer_dataframe_depuis_multiple_url()`
- `creer_dataframe_depuis_melodi_api_url()`

Sortie :
```

Raw DataFrame (non nettoyé)

```

---

# 3. Silver Layer – Transformations

Chaque dataset passe par une fonction de nettoyage dédiée :

```

Raw → clean_xxx() → Silver DataFrame

```

Transformations communes :

- normalisation des noms de colonnes
- correction des codes département
- conversion des types numériques
- gestion des valeurs manquantes
- agrégation annuelle
- filtrage des années
- enrichissement via métadonnées JSON

---

# 4. Lineage détaillé par dataset

---

## 4.1 Délinquance

**Source**
- Parquet Data.gouv

**Pipeline**
```

URL delinquance
→ creer_dataframe_depuis_parquet_url()
→ clean_delinquance()
→ silver_delinquance_df

```

Transformations :
- mapping infractions via metadata
- agrégation par département / année

---

## 4.2 Taux de chômage

**Source**
- Excel INSEE (trimestriel)

**Pipeline**
```

URL taux_chommage
→ creer_dataframe_depuis_xls_url()
→ clean_taux_chomage()
→ silver_taux_chommage_df

```

Transformations :
- extraction feuille "Département"
- moyenne des 4 trimestres

---

## 4.3 Âge moyen

**Source**
- API INSEE Melodi

**Pipeline**
```

API Melodi
→ creer_dataframe_depuis_melodi_api_url()
→ clean_age_moyen()
→ silver_age_moyen_df

```

---

## 4.4 Population active

**Source**
- API Melodi
- metadata_population_active.json

**Pipeline**
```

API
→ raw dataframe
→ mapping statuts
→ clean_population_active()
→ silver_population_active_df

```

---

## 4.5 Catégories professionnelles

**Source**
- API INSEE
- metadata_categorie_professionnelle.json

```

API
→ mapping PCS
→ clean_categorie_professionnelle()
→ silver_categorie_professionnelle_df

```

---

## 4.6 Équipements sportifs

**Source**
- Parquet Data.gouv

```

Parquet
→ clean_equipement_sportif()
→ silver_equipement_sportif_df

```

Transformations :
- calcul du stock annuel
- filtrage année ≥ 1950

---

## 4.7 Revenu moyen

**Sources**
- XLSX historique (1984–2020)
- ZIP récents (2021–2023)

```

Multiple URLs
→ extraction ZIP/XLSX
→ harmonisation formats
→ clean_revenu_moyen()
→ silver_revenu_moyen_df

```

---

## 4.8 Établissements culturels

```

Parquet
→ clean_etablissement_culturel()
→ silver_etablissement_culturel_df

```

---

## 4.9 Pouvoir d’achat

```

Excel INSEE
→ clean_pouvoir_achat()
→ silver_pouvoir_achat_df

```

---

## 4.10 Niveau d’étude

**Source**
- API Melodi
- metadata_niveau_etude.json

```

API
→ mapping diplômes
→ pivot / agrégation
→ clean_niveau_etude()
→ silver_niveau_etude_df

```

---

## 4.11 Abstention

```

Parquet élections
→ clean_abstention_votant()
→ silver_abstention_votant_df

```

Transformations :
- agrégation département
- calcul participation / abstention

---

## 4.12 Président sortant

**Source**
- Résultats candidats
- metadata_famille_politique.json

```

Parquet
→ mapping candidat → famille politique
→ clean_president_sortant()
→ silver_president_sortant_df

```

---

# 5. Métadonnées (enrichissement)

| Fichier | Utilisation |
|---|---|
| metadata_delinquance.json | typologie infractions |
| bords_politiques.json | famille politique |
| population_active.json | statuts emploi |
| categorie_professionnelle.json | PCS |
| niveau_etude.json | diplômes |

---

# 6. Clé de convergence Silver

Clé standard :

```

Code_departement (string)
annee (int)

```

Cette clé permet la future fusion en Gold.

Certaines tables ont des dimensions supplémentaires :
- tour (élections)
- catégorie
- statut

---

# 7. Stockage

Tous les DataFrames Silver sont persistés via :

```

save_all_silver_dataframes(dataframes)

```

Format recommandé :
- Parquet
- 1 fichier par dataset

---

# 8. Qualité et audit (optionnel)

Fonction disponible :

```

audit_all_silver_dataframes()

```

Contrôles possibles :
- doublons
- valeurs manquantes
- valeurs négatives
- couverture départements

---

# 9. Lineage global synthétique

```

Data.gouv / INSEE / Ministères
↓
Raw Requesters
↓
clean_xxx()
↓
Silver DataFrames
↓
save_all_silver_dataframes()
↓
Stockage Parquet
↓
(Future Gold)

```

---

# 10. Évolutions recommandées

- Ajout d’un Gold fusionné (dept × année)
```