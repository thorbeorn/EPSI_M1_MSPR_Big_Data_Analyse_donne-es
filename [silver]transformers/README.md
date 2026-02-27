# 📊 Module de nettoyage et transformation des données socio-économiques et électorales

Ce module regroupe l’ensemble des fonctions de **nettoyage, normalisation et transformation** utilisées dans le pipeline de données.

Il permet de convertir des fichiers bruts (Excel, CSV, JSON, SDMX) issus de différentes sources institutionnelles (INSEE, DGFiP, Ministère de l’Intérieur, etc.) en **DataFrames normalisés**, prêts pour :

* la fusion inter-sources
* l’analyse statistique
* la modélisation
* la visualisation

---

## 🧱 Philosophie du module

Chaque fonction :

* Prend en entrée **un DataFrame brut**
* Applique les règles de nettoyage spécifiques à la source
* Retourne un **DataFrame propre, structuré et harmonisé**

Le module est conçu pour :

* Être **idempotent**
* Être **robuste aux formats hétérogènes**
* Centraliser toute la logique métier
* Faciliter la maintenance du pipeline

---

## 📌 Conventions de nommage

### 🔑 Clé de jointure commune

Toutes les tables sont harmonisées autour de :

```text
Code_departement + annee
```

---

### 🏷️ Préfixage des colonnes

Chaque variable issue d’une source est préfixée :

```
[nom_source]nom_variable
```

Exemples :

* `[delinquance]nombre`
* `[taux_chomage]Taux_moyen`
* `[revenu_moyen]revenu_moyen_par_foyer`
* `[niveau_etude]Baccalauréat universitaire ou équivalent`

Cela permet :

* D’éviter les collisions de colonnes
* D’identifier immédiatement la provenance d’une variable
* De garder un schéma explicite

---

# 🧰 Utilitaires génériques

### `normaliser(texte: str) -> str`

Normalisation Unicode :

* Minuscule
* Suppression des accents
* Suppression des diacritiques

Permet des comparaisons robustes (JSON mapping, candidats, codes).

---

### `clean_excel_block(df, skip_rows, drop_last=0)`

Nettoie un bloc Excel mal formaté :

* Ignore les lignes de métadonnées
* Replace correctement l’en-tête
* Supprime les lignes de totaux

---

### `load_json_mapping(path, key_field, value_field, normalize=True)`

Construit un dictionnaire de mapping depuis un JSON :

```
clé → valeur
```

Utilisé pour :

* Familles politiques
* Statuts d’emploi
* Catégories professionnelles
* Diplômes

---

### `fix_departement(code)`

Corrige les anomalies fréquentes :

* `750 → 75`
* `2A0 → 2A`
* DOM-TOM préservés
* Suppression des codes fictifs DGFiP

---

### `_parse_numeric_col(series)`

Convertit des colonnes Excel contenant :

* Espaces milliers : `"1 234 567"`
* Virgules décimales : `"12,5"`

en float propre.

---

# 🗂 Fonctions par source

---

## 📈 Indicateurs socio-économiques

### `clean_delinquance(df)`

Agrège les faits de délinquance par département et année.

* Somme des faits
* Moyenne du taux pour mille

---

### `clean_taux_chomage(df)`

Transforme un fichier Excel trimestriel en :

* Taux annuel moyen par département

Pipeline :

1. Nettoyage Excel
2. Melt (wide → long)
3. Extraction année
4. Moyenne annuelle

---

### `clean_revenu_moyen(dfs)`

Consolide les revenus fiscaux moyens 1984–2023.

Particularités :

* Formats DGFiP très hétérogènes
* Agrégation commune → département (2021–2023)
* Correction des codes départements
* Gestion des feuilles multiples

Produit :

```
[revenu_moyen]revenu_moyen_par_foyer
```

---

### `clean_pouvoir_achat(df)`

Nettoie la série nationale de variation annuelle du pouvoir d’achat.

Produit :

```
[pouvoir_achat]pourcentage_annee_precedente
```

---

### `clean_population_active(df, metadata)`

Population active par :

* Département
* Année
* Statut d’emploi
* Tranche d’âge

Pivot multi-index + mapping JSON.

---

### `clean_categorie_professionnelle(df, metadata)`

Catégories socio-professionnelles (PCS) :

* Suppression des agrégats `_T`
* Mapping code → libellé
* Pivot par année

---

### `clean_niveau_etude(df, metadata)`

Diplômes par département et année.

* Harmonisation des codes redondants
* Mapping JSON
* Pivot par diplôme

---

### `clean_age_moyen(df)`

Âge moyen population active par tranche :

* 15–24
* 25–54
* 55+

Gestion spécifique de la Corse pour tri numérique stable.

---

### `clean_equipement_sportif(df)`

Calcule le **stock actif annuel** d’équipements sportifs.

Méthode avancée par deltas :

* +1 à l’entrée
* -1 à la sortie
* Somme cumulée

Avantage : mémoire optimisée.

---

### `clean_etablissement_culturel(df)`

Nettoyage simple :

* Suppression colonnes calculées
* Harmonisation noms
* Conversion en int

---

## 🗳 Indicateurs électoraux

### `clean_president_sortant(df, metadata_famille_politique)`

Extrait les résultats présidentiels (T1 & T2).

Particularités :

* Décalage année −1
* Mapping candidat → famille politique
* Harmonisation DOM-TOM
* Suppression votes étranger (ZZ)

---

### `clean_abstention_votant(df)`

Agrège les données participation :

* Inscrits
* Abstentions
* Blancs
* Nuls

Agrégation BV → département.

---

# 🧠 Logique métier importante

## 📆 Décalage temporel

Certaines variables sont décalées :

```
année = année_source - 1
```

Raison :
Associer les variables socio-économiques à l’année précédant l’élection.

Appliqué à :

* Présidentielle
* Abstention
* Population active
* Niveau d’étude

---

## 🏝 Gestion des DOM-TOM

Mapping spécifique :

```
ZA → 971
ZB → 972
...
```

Votes étrangers (`ZZ`) supprimés.

---

## 🧪 Robustesse

Le module :

* Vérifie la présence des colonnes requises
* Log les erreurs via `logging`
* Lève explicitement les exceptions importantes
* Utilise des conversions numériques sécurisées (`errors="coerce"`)

---

# 📦 Dépendances

```bash
pip install pandas
```

Librairies utilisées :

* `pandas`
* `json`
* `unicodedata`
* `logging`

---

# 🔄 Intégration dans un pipeline

Exemple simplifié :

```python
df_chomage = clean_taux_chomage(raw_chomage)
df_revenus = clean_revenu_moyen(dfs_revenus)

df_final = (
    df_chomage
    .merge(df_revenus, on=["Code_departement", "annee"], how="left")
)
```

---

# 🎯 Objectif final

Produire un dataset :

* Cohérent
* Historique (multi-années)
* Inter-sources
* Prêt pour :

  * Analyse exploratoire
  * Modélisation prédictive
  * Analyse électorale
  * Études socio-économiques territoriales