"""
Module de nettoyage et transformation des données socio-économiques et électorales.

Ce module regroupe toutes les fonctions de nettoyage (cleaning) utilisées dans
le pipeline de données. Chaque fonction prend en entrée un DataFrame brut issu
d'une source externe (Excel, CSV, JSON) et retourne un DataFrame normalisé,
prêt pour la fusion et l'analyse.

Conventions de nommage des colonnes :
    - Les colonnes issues d'une source spécifique sont préfixées par [nom_source]
    - La clé de jointure commune est toujours `Code_departement` + `annee`
"""

import pandas as pd
import json
import unicodedata
import logging
from functools import reduce

# ─── Logger ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# UTILITAIRES GÉNÉRIQUES
def normaliser(texte: str) -> str:
    """
    Normalise une chaîne de caractères pour les comparaisons insensibles
    à la casse et aux accents.

    Opérations appliquées :
        1. Conversion en minuscules.
        2. Décomposition Unicode (NFD) pour isoler les diacritiques.
        3. Suppression des caractères de catégorie Unicode 'Mn' (marques non-espacées).

    Args:
        texte (str): La chaîne à normaliser.

    Returns:
        str: La chaîne normalisée, sans majuscules ni accents.

    Raises:
        TypeError: Si `texte` n'est pas une chaîne de caractères.

    Exemple:
        >>> normaliser("Île-de-France")
        'ile-de-france'
    """
    try:
        texte = texte.lower()
        # Décomposition NFD : sépare chaque lettre accentuée en lettre + diacritique
        texte = unicodedata.normalize('NFD', texte)
        # Supprime uniquement les diacritiques (catégorie Mn = Mark, Nonspacing)
        texte = ''.join(char for char in texte if unicodedata.category(char) != 'Mn')
        return texte
    except AttributeError as e:
        logger.error(f"normaliser() : entrée invalide (non-string) → {e}")
        raise TypeError(f"normaliser() attend une str, reçu : {type(texte)}") from e

def clean_excel_block(df: pd.DataFrame, skip_rows: int, drop_last: int = 0) -> pd.DataFrame:
    """
    Nettoie un bloc Excel mal formaté dont les vraies en-têtes ne sont pas
    sur la première ligne.

    Problème courant : les fichiers Excel exportés contiennent des lignes
    de titre ou de métadonnées avant les vraies colonnes.

    Args:
        df (pd.DataFrame): DataFrame brut issu de pd.read_excel().
        skip_rows (int): Nombre de lignes à ignorer avant l'en-tête réelle.
        drop_last (int): Nombre de lignes de pied de tableau à supprimer
                         (totaux, notes, etc.). Par défaut 0.

    Returns:
        pd.DataFrame: DataFrame nettoyé avec les bonnes colonnes.

    Raises:
        ValueError: Si skip_rows est négatif ou supérieur à la taille du DataFrame.
        IndexError: Si le DataFrame n'a pas assez de lignes après le skip.
    """
    try:
        if skip_rows < 0:
            raise ValueError(f"skip_rows doit être >= 0, reçu : {skip_rows}")
        if skip_rows >= len(df):
            raise ValueError(
                f"skip_rows ({skip_rows}) >= nombre de lignes ({len(df)})"
            )

        # Saute les lignes de métadonnées initiales
        df = df.iloc[skip_rows:]

        # La première ligne restante devient l'en-tête
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)
        df.columns.name = None  # Supprime le nom de l'axe colonnes (artefact pandas)

        # Supprime les lignes de pied (totaux, notes en bas de tableau)
        if drop_last:
            df = df.iloc[:-drop_last]

        return df

    except (ValueError, IndexError) as e:
        logger.error(f"clean_excel_block() : erreur de nettoyage → {e}")
        raise

def load_json_mapping(
    path: str,
    key_field: str,
    value_field: str,
    normalize: bool = True
) -> dict:
    """
    Charge un fichier JSON et construit un dictionnaire de mapping clé → valeur.

    Utilisé pour mapper des codes (ex: codes PCS, codes diplômes) vers leurs
    libellés humains, ou des noms de candidats vers leur famille politique.

    Args:
        path (str): Chemin vers le fichier JSON.
        key_field (str): Nom du champ à utiliser comme clé du dictionnaire.
        value_field (str): Nom du champ à utiliser comme valeur.
        normalize (bool): Si True, les clés sont normalisées via `normaliser()`.
                          Utile pour des comparaisons robustes. Par défaut True.

    Returns:
        dict: Dictionnaire {clé: valeur} construit depuis le JSON.

    Raises:
        FileNotFoundError: Si le fichier JSON n'existe pas.
        KeyError: Si `key_field` ou `value_field` sont absents d'un item.
        json.JSONDecodeError: Si le fichier n'est pas un JSON valide.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if normalize:
            return {
                normaliser(item[key_field]): item[value_field]
                for item in data
            }
        return {item[key_field]: item[value_field] for item in data}

    except FileNotFoundError:
        logger.error(f"load_json_mapping() : fichier introuvable → {path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"load_json_mapping() : JSON invalide dans {path} → {e}")
        raise
    except KeyError as e:
        logger.error(f"load_json_mapping() : champ manquant dans le JSON → {e}")
        raise

def fix_departement(code) -> str:
    """
    Corrige et normalise les codes départementaux issus de sources fiscales.

    Problème : dans les fichiers de revenus (DGFiP), les codes département
    sont parfois encodés comme des multiples de 10 (ex: 750 → 75),
    ou présentent des suffixes parasites pour la Corse (2A0, 2B0).

    Traitement :
        - '2A0' → '2A'  /  '2B0' → '2B'  (Corse)
        - Codes >= 970 : retournés tels quels (DOM-TOM)
        - Autres : division entière par 10 + formatage sur 2 chiffres

    Args:
        code: Le code département brut (str, int ou float acceptés).

    Returns:
        str: Le code département normalisé.

    Exemple:
        >>> fix_departement('750')
        '75'
        >>> fix_departement('2A0')
        '2A'
    """
    try:
        code = str(code).strip()

        # Cas Corse avec suffixe parasite
        if code in ('2A0', '2B0'):
            return code[:-1]  # Supprime le '0' final

        # Tentative de conversion en entier pour les codes numériques
        try:
            code_int = int(code)
        except ValueError:
            # Code non numérique non-Corse : retourné tel quel (ex: '2A', '2B')
            return code

        # DOM-TOM : codes >= 970, on garde tel quel
        if code_int >= 970:
            return code

        # Codes DGFiP encodés ×10 : on divise et on zero-pad à 2 chiffres
        return str(code_int // 10).zfill(2)

    except Exception as e:
        logger.warning(f"fix_departement() : impossible de traiter '{code}' → {e}")
        return str(code)

def _parse_numeric_col(series: pd.Series) -> pd.Series:
    """
    Convertit une colonne de valeurs textuelles (issues d'Excel) en float.

    Gère les cas courants :
        - Espaces comme séparateurs de milliers (ex: '1 234 567')
        - Virgules comme séparateur décimal (ex: '12,5')

    Args:
        series (pd.Series): Colonne à convertir.

    Returns:
        pd.Series: Colonne numérique (float). Les valeurs non convertibles
                   deviennent NaN (errors='coerce').
    """
    try:
        return pd.to_numeric(
            series.astype(str)
                  .str.replace(" ", "")   # Supprime les espaces (séparateurs milliers)
                  .str.replace(",", "."),  # Virgule décimale → point
            errors="coerce"
        )
    except Exception as e:
        logger.error(f"_parse_numeric_col() : erreur de conversion → {e}")
        raise

def _set_header(df: pd.DataFrame, skip_rows: int) -> pd.DataFrame:
    """
    Version simplifiée de `clean_excel_block` sans suppression de fin.
    Positionne l'en-tête correctement après avoir sauté des lignes initiales.

    Args:
        df (pd.DataFrame): DataFrame brut.
        skip_rows (int): Nombre de lignes à ignorer avant l'en-tête.

    Returns:
        pd.DataFrame: DataFrame avec les bonnes colonnes.
    """
    try:
        df = df.iloc[skip_rows:]
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)
        df.columns.name = None
        return df
    except Exception as e:
        logger.error(f"_set_header() : erreur → {e}")
        raise

# FONCTIONS DE NETTOYAGE PAR SOURCE
def clean_delinquance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège les données de délinquance par département et par année.

    Source : données INSERM / Ministère de l'Intérieur.
    Les données sont fournies par type d'infraction, on les agrège donc
    en sommant le nombre de faits et en moyennant le taux pour mille.

    Colonnes requises :
        - Code_departement : code INSEE du département
        - annee            : année des faits
        - nombre           : nombre de faits enregistrés
        - taux_pour_mille  : taux pour 1000 habitants

    Colonnes produites :
        - [delinquance]nombre          : somme des faits par dept/année
        - [delinquance]taux_pour_mille : moyenne du taux par dept/année

    Args:
        df (pd.DataFrame): DataFrame brut avec les colonnes requises.

    Returns:
        pd.DataFrame: Données agrégées par (Code_departement, annee).

    Raises:
        ValueError: Si des colonnes requises sont manquantes.
    """
    try:
        # Vérification de la présence des colonnes obligatoires
        required_cols = {"Code_departement", "annee", "nombre", "taux_pour_mille"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Colonnes manquantes dans clean_delinquance : {missing}")
        
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("int64") + 1  # Décalage temporel : on associe à l'année N-1
        return (
            df
            .groupby(
                ["Code_departement", "annee"],
                as_index=False,
                sort=False,
                observed=True  # Ignore les catégories non présentes (perf)
            )
            .agg(
                **{
                    # Somme des faits (chaque ligne = un type d'infraction)
                    "[delinquance]nombre": ("nombre", "sum"),
                    # Moyenne du taux (le taux global n'est pas la somme des taux)
                    "[delinquance]taux_pour_mille": ("taux_pour_mille", "mean"),
                }
            )
        )
    except ValueError:
        raise
    except Exception as e:
        logger.error(f"clean_delinquance() : erreur inattendue → {e}")
        raise

def clean_taux_chomage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie et restructure les données du taux de chômage trimestriel
    en une table annuelle par département.

    Problème d'entrée :
        - Fichier Excel avec 2 lignes de métadonnées en tête
        - Format wide : une colonne par période (ex: 'T1 2020', 'T2 2020'…)
        - 4 lignes de totaux en bas

    Transformations :
        1. Nettoyage de l'en-tête Excel (skip 2 lignes, drop 4 en bas)
        2. Pivot wide → long (melt)
        3. Extraction de l'année depuis la période (ex: 'T3 2019' → 2019)
        4. Agrégation trimestrielle → moyenne annuelle

    Colonnes produites :
        - Code_departement            : code sur 2 chiffres (zero-padded)
        - annee                       : année (int)
        - [taux_chomage]Taux_moyen    : moyenne annuelle du taux de chômage

    Args:
        df (pd.DataFrame): DataFrame brut issu du fichier Excel DARES/INSEE.

    Returns:
        pd.DataFrame: Taux de chômage moyen annuel par département.
    """
    try:
        # Nettoyage de l'en-tête spécifique à ce fichier Excel
        df = clean_excel_block(df, skip_rows=2, drop_last=4)

        # Supprime la colonne "Libellé" (libellé textuel du département, inutile)
        df = df.drop(columns="Libellé", errors="ignore")

        # Passage du format wide (une colonne par trimestre) au format long
        df = df.melt(
            id_vars="Code",        # Identifiant département (garde tel quel)
            var_name="Periode",    # Nom de l'ancienne colonne (ex: 'T1 2020')
            value_name="Taux"      # Valeur du taux de chômage
        )

        # Normalisation du code département sur 2 chiffres
        df["Code"] = df["Code"].astype(str).str.zfill(2)

        # Extraction de l'année depuis le libellé de période (4 derniers caractères)
        df["annee"] = df["Periode"].str[-4:].astype(int)

        # Sécurisation numérique (les valeurs Excel peuvent être des strings)
        df["Taux"] = pd.to_numeric(df["Taux"], errors="coerce")
        df = df.dropna(subset=["Taux"]).reset_index(drop=True)
        # Agrégation trimestrielle vers annuelle (moyenne des 4 trimestres)
        df = (
            df
            .groupby(["Code", "annee"], as_index=False, sort=False)
            .agg(**{"[taux_chomage]Taux_moyen": ("Taux", "mean")})
        )
        return df.rename(columns={"Code": "Code_departement"})
    except Exception as e:
        logger.error(f"clean_taux_chomage() : erreur → {e}")
        raise

def clean_age_moyen(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données d'âge moyen de la population active par tranche d'âge
    et par département, issues de l'INSEE (format Eurostat/SDMX).

    Note Corse : les codes '2A' et '2B' sont remplacés par des entiers
    fictifs (1000, 1001) pour permettre un tri numérique stable.

    Args:
        df (pd.DataFrame): DataFrame brut issu du fichier INSEE (format SDMX).

    Returns:
        pd.DataFrame: Données pivotées avec une colonne par tranche d'âge.
    """
    try:
        # Supprime les colonnes de métadonnées inutiles pour l'analyse
        df = df.drop(columns=["dep_l", "newreg", "newreg_l"], errors="ignore")
        # On supprime la distinction sexe et on agrège.
        df = (
            df.groupby(["dep", "trage", "annee"], as_index=False)
            .agg(pop_totale=("pop", "sum"))
        )
        # Extraction du code département depuis le code géographique
        df["Code_departement"] = df["dep"]
        # Construction d'une série de tri numérique pour gérer la Corse
        # (2A et 2B ne sont pas triables numériquement directement)
        sort_series = (
            df["Code_departement"]
            .replace({"2A": "1000", "2B": "1001"})
            .astype(int)
        )
        # Tri stable (mergesort) pour préserver l'ordre en cas d'égalité
        df = (
            df
            .assign(_sort=sort_series)
            .sort_values("_sort", kind="mergesort")
            .drop(columns=["_sort", "dep"])
        )
        # Pivot : transforme les lignes (une par tranche d'âge) en colonnes
        df = (
            df
            .pivot_table(
                index=["Code_departement", "annee"],
                columns="trage",
                values="pop_totale",
                aggfunc="first"  # Valeur unique attendue par cellule
            )
            .reset_index()
        )
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("int64")
        df.columns.name = None  # Supprime l'artefact "AGE" sur l'axe colonnes
        # Colonnes à exclure
        colonnes_fixes = ["Code_departement", "annee"]
        # Renommer automatiquement toutes les autres colonnes
        return df.rename(
            columns={
                col: f"[age_moyen]{col}"
                for col in df.columns
                if col not in colonnes_fixes
            }
        )
    except Exception as e:
        logger.error(f"clean_age_moyen() : erreur → {e}")
        raise

def clean_president_sortant(df: pd.DataFrame, metadata_famille_politique: str ) -> pd.DataFrame:
    """
    Extrait les informations sur les candidats des élections présidentielles
    (T1 et T2) et les enrichit avec leur famille politique.

    Logique métier :
        - On décale l'année de -1 pour associer les résultats électoraux
          à l'année précédant l'élection (utilisé comme variable explicative
          pour les données de l'année suivante).
        - La famille politique est mappée via un fichier JSON externe.

    Colonnes produites :
        - code_departement
        - [president_sortant]tour
        - [president_sortant]candidat
        - [president_sortant]famille_politique
        - [president_sortant]nombre_voix

    Args:
        df (pd.DataFrame): Données brutes électorales.
        metadata_famille_politique (str): Chemin vers le JSON de mapping
                                          candidat → famille politique.

    Returns:
        pd.DataFrame: Données candidates nettoyées et enrichies.
    """
    try:
        # Filtre uniquement les élections présidentielles (T1 et T2)
        mask = df["id_election"].str.contains("pres_t", na=False)
        df = df.loc[mask].copy()
        # Extraction de l'année et du tour depuis l'identifiant d'élection
        # Format : '2022_pres_t1' → année='2022', tour='t1'
        df[["annee", "tour"]] = df["id_election"].str.extract(
            r"(\d{4})_pres_(t[12])",
            expand=True
        )
        # Décalage temporel : on associe les candidats à l'année N-1
        df["annee"] = df["annee"].astype(int)
        # Suppression des colonnes sans valeur analytique
        df = df.drop(
            columns=[
                "id_election", "id_brut_miom", "code_commune", "code_bv",
                "nuance", "sexe", "no_panneau",
                "ratio_voix_inscrits", "ratio_voix_exprimes",
                "libelle_abrege_liste", "nom_tete_liste",
                "binome", "liste", "libelle_etendu_liste"
            ],
            errors="ignore"
        )
        # Sélection des colonnes utiles
        df = df[["code_departement", "annee", "tour", "nom", "prenom", "voix"]]
        # Dédoublonnage avant fusion (une ligne par candidat par département)
        df = df.drop_duplicates(ignore_index=True)
        # Construction du nom complet du candidat (vectorisé, sans boucle)
        df["candidat"] = df["nom"].str.cat(df["prenom"], sep=" ")
        df = df.drop(columns=["nom", "prenom"])
        # Chargement du mapping candidat → famille politique depuis JSON
        mapping = load_json_mapping(
            metadata_famille_politique,
            key_field="nom",
            value_field="famille_politique",
            normalize=True  # Comparaison insensible aux accents/casse
        )
        # Application du mapping (normalisation préalable du candidat)
        df["famille_politique"] = (
            df["candidat"]
            .astype(str)
            .apply(normaliser)
            .map(mapping)
        )
        # Renommage des colonnes avec préfixe source
        df = df.rename(columns={
            "tour": "[president_sortant]tour",
            "candidat": "[president_sortant]candidat",
            "famille_politique": "[president_sortant]famille_politique",
            "voix": "[president_sortant]nombre_voix"
        })
        # Harmonisation des codes DOM-TOM (format lettré → numérique INSEE)
        df["code_departement"] = df["code_departement"].replace({
            "ZA": "971",  # Guadeloupe
            "ZB": "972",  # Martinique
            "ZC": "973",  # Guyane
            "ZD": "974",  # La Réunion
            "ZM": "976",  # Mayotte
            "ZN": "988",  # Nouvelle-Calédonie
            "ZP": "987",  # Polynésie française
            "ZS": "975",  # Saint-Pierre-et-Miquelon
            "ZT": "978",  # Saint-Martin / Saint-Barthélemy
            "ZW": "986",  # Wallis-et-Futuna
            "ZX": "977",  # Saint-Barthélemy
            "ZY": "977",  # Saint-Martin (fusionné avec ZX → 977)
        })
        # Suppression des votes de l'étranger (ZZ) non rattachés à un département
        df = df[df.iloc[:, 0] != "ZZ"]
        # Tri final par département puis année (stable)
        df = df.sort_values(
            ["code_departement", "annee"],
            kind="mergesort"
        ).reset_index(drop=True)
        return df
    except Exception as e:
        logger.error(f"clean_president_sortant() : erreur → {e}")
        raise

def clean_compte_publique(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données de des comptes publique par département.

    Colonnes produites :
        - Code_departement
        - annee
        - Compte

    Args:
        df (pd.DataFrame): Données brutes.

    Returns:
        pd.DataFrame: Population active structurée par dept/année/statut.
    """
    try:
        # Suppression des colonnes de métadonnées inutiles
        df = df.drop(
            columns=["outre_mer", "reg_code", "reg_name", "dep_tranche_population", "dep_status", "dep_name", "categ", "siren", "ident", "lbudg", "type_de_budget", "nomen", "agregat", "classement_fonctionnel_2", "fonction2", "cbudg", "nom_fonction", "fonction", "fonctionnelle_1", "niveau_hierarchique", "ptot_n"],
            errors="ignore"
        )
        # Extraction du code département depuis la colonne géographique
        df["exer"] = pd.to_datetime(df["exer"])
        df["annee"] = df["exer"].dt.year
        # Suppression des colonnes inutiles
        df = df.drop(
            columns=["exer"],
            errors="ignore"
        )

        df = (
            df.groupby(["annee", "dep_code"], as_index=False)
            .agg({
                "montant": "sum",
                "ptot": "sum",
                "euros_par_habitant": "mean"
            })
        )
        # Renommage des colonnes
        df = df.rename(columns={
            "dep_code": "Code_departement",
            "montant": "[compte_publique]depenses",
            "ptot": "[compte_publique]population",
            "euros_par_habitant": "[compte_publique]euros_par_habitant"
        })
        # Réorganisation des colonnes dans l'ordre final
        df = (
            df
            .loc[:, [
                "Code_departement",
                "annee",
                "[compte_publique]depenses",
                "[compte_publique]population",
                "[compte_publique]euros_par_habitant"
            ]]
            .fillna(0)     # Les valeurs manquantes représentent 0 actifs
            .reset_index(drop=True)
        )
        print(df)

        return df

    except Exception as e:
        logger.error(f"clean_population_active() : erreur → {e}")
        raise

def clean_categorie_professionnelle(
    df: pd.DataFrame,
    metadata_categorie_professionnelle: str
) -> pd.DataFrame:
    """
    Nettoie et pivote les données de catégorie socio-professionnelle (PCS)
    issues de l'INSEE (format SDMX/Eurostat).

    Particularités :
        - Les lignes avec PCS '_T' (total) sont supprimées pour éviter
          les doubles comptes lors de l'agrégation.
        - Les données ne sont pas filtrées par département (table nationale).

    Colonnes produites :
        - annee
        - [categorie_professionnelle] <libellé PCS> : une colonne par CSP

    Args:
        df (pd.DataFrame): Données brutes INSEE format SDMX.
        metadata_categorie_professionnelle (str): Chemin vers le JSON
                                                   code PCS → libellé.

    Returns:
        pd.DataFrame: Données pivotées par année et catégorie professionnelle.
    """
    try:
        # Suppression des colonnes de métadonnées sans valeur analytique
        df = df.drop(
            columns=[
                'IMMI', 'EEC_MEASURE', 'SEX', 'EDUC', 'UNDEREMP',
                'EMPFORM', 'UNEMPDUR', 'COMPOHALO', 'EMPSTA',
                'WKTIME', 'ACTIVITY', 'AGE', 'OBS_STATUS',
                'UNIT_MULT', 'UNIT_MEASURE'
            ],
            errors="ignore"
        )
        # Conversion en numérique et tri chronologique
        df["TIME_PERIOD"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce")
        df = df.sort_values("TIME_PERIOD", kind="mergesort")
        # Suppression des lignes de total PCS ('_T' = agrégat de toutes les CSP)
        # Ces totaux provoqueraient des doubles comptes dans les aggrégations
        df = df[~df["PCS"].str.contains("_T", na=False)]
        # Chargement du mapping code PCS → libellé depuis JSON
        with open(metadata_categorie_professionnelle, "r", encoding="utf-8") as f:
            mapping = {
                normaliser(item["code"]): item["libelle"]
                for item in json.load(f)
            }
        # Application du mapping (normalisation préalable)
        df["PCS"] = (
            df["PCS"]
            .astype(str)
            .apply(normaliser)
            .map(mapping)
        )
        # Pivot : une ligne par année, une colonne par catégorie PCS
        df = (
            df
            .pivot_table(
                index="TIME_PERIOD",
                columns="PCS",
                values="OBS_VALUE_NIVEAU",
                aggfunc="sum"    # Somme des effectifs par PCS/année
            )
            .reset_index()
        )
        df.columns.name = None
        # Renommage de l'index temporel
        df = df.rename(columns={"TIME_PERIOD": "annee"})
        # Ajout du préfixe source sur toutes les colonnes sauf 'annee'
        df.columns = [
            col if col == "annee"
            else f"[categorie_professionnelle] {col}"
            for col in df.columns
        ]
        df = df.rename(columns={"[categorie_professionnelle] Artisans, commerçants et chefs d’entreprise": "[categorie_professionnelle] Artisans, commerçants et patron", "[categorie_professionnelle] Cadres et professions intellectuelles supérieures": "[categorie_professionnelle] Cadres et professions supérieures"})
        return df.reset_index(drop=True)
    except FileNotFoundError:
        logger.error(
            f"clean_categorie_professionnelle() : JSON introuvable → "
            f"{metadata_categorie_professionnelle}"
        )
        raise
    except Exception as e:
        logger.error(f"clean_categorie_professionnelle() : erreur → {e}")
        raise

def clean_equipement_sportif(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le nombre d'équipements sportifs actifs par département et par année,
    en utilisant une approche basée sur les deltas (entrées/sorties).

    Algorithme :
        1. Détermine l'année de mise en service de chaque équipement.
        2. Détermine l'année de fin de service (hors-service ou aujourd'hui).
        3. Calcule les variations nettes (+1 à l'entrée, -1 à la sortie).
        4. Cumule ces variations pour obtenir le stock actif année par année.

    Avantage : évite de stocker une ligne par équipement par année,
    réduit drastiquement la mémoire nécessaire.

    Colonnes produites :
        - Code_departement
        - annee
        - [equipement_sportif]nb_equipements : stock actif en fin d'année

    Args:
        df (pd.DataFrame): Données brutes du Recensement des Équipements Sportifs (RES).

    Returns:
        pd.DataFrame: Stock annuel d'équipements par département depuis 1950.
    """
    try:
        # Sélection des colonnes nécessaires au calcul
        df = df[[
            "dep_code",
            "equip_service_date",     # Date précise de mise en service
            "equip_service_periode",  # Période approximative si pas de date
            "inst_hs_bool",           # Booléen : équipement hors-service ?
            "inst_date_etat"          # Date de mise hors-service
        ]].copy()
        # ── Calcul de l'année de mise en service ────────────────────────────
        df["equip_service_date"] = pd.to_datetime(df["equip_service_date"], errors="coerce")
        annee_date = df["equip_service_date"].dt.year
        # Extraction des années depuis la période (format : '1985' ou '1985-1990')
        # On prend la fin de la période si disponible, sinon le début
        annees = (
            df["equip_service_periode"]
            .str.extract(r"(\d{4})(?:-(\d{4}))?")
            .astype(float)
        )
        annee_periode = annees[1].fillna(annees[0])
        # Priorité à la date précise, fallback sur la période
        df["equip_service_annee"] = (
            annee_date.fillna(annee_periode)
            .astype("Int64")  # Entier nullable (supporte NaN)
        )
        # ── Calcul de l'année de fin de service ─────────────────────────────
        annee_actuelle = pd.Timestamp.now().year
        df["inst_date_etat"] = pd.to_datetime(df["inst_date_etat"], errors="coerce")
        annee_etat = df["inst_date_etat"].dt.year
        # Par défaut, on suppose que l'équipement est encore actif cette année
        df["dern_inst_date"] = annee_actuelle
        # Pour les équipements hors-service, on utilise la date de mise HS
        df.loc[df["inst_hs_bool"] == True, "dern_inst_date"] = annee_etat
        df["dern_inst_date"] = df["dern_inst_date"].astype("Int64")
        # ── Construction des deltas entrée/sortie ───────────────────────────
        df = df[["dep_code", "equip_service_annee", "dern_inst_date"]]
        # Delta positif : +1 équipement l'année de mise en service
        debut = (
            df.groupby(["dep_code", "equip_service_annee"])
            .size()
            .reset_index(name="delta")
            .rename(columns={"equip_service_annee": "annee"})
        )
        # Delta négatif : -1 équipement l'année APRÈS la fin de service
        # (l'équipement compte encore pour sa dernière année d'activité)
        fin = (
            df.groupby(["dep_code", "dern_inst_date"])
            .size()
            .reset_index(name="delta")
            .rename(columns={"dern_inst_date": "annee"})
        )
        fin["annee"] += 1      # Décalage d'un an
        fin["delta"] *= -1     # Variation négative
        # Fusion des deux séries de deltas
        variations = pd.concat([debut, fin], ignore_index=True)
        # ── Construction de la grille complète dept × année ─────────────────
        annee_min = df["equip_service_annee"].min()
        annee_max = df["dern_inst_date"].max()
        # Produit cartésien : tous les départements × toutes les années
        base = (
            pd.MultiIndex.from_product(
                [df["dep_code"].unique(), range(annee_min, annee_max + 1)],
                names=["dep_code", "annee"]
            )
            .to_frame(index=False)
            .merge(variations, on=["dep_code", "annee"], how="left")
        )
        # Les années sans variation ont un delta de 0
        base["delta"] = base["delta"].fillna(0)
        # ── Cumul des deltas → stock actif ───────────────────────────────────
        base["nb_equipements"] = (
            base.groupby("dep_code")["delta"]
            .cumsum()  # Somme cumulée = stock à chaque instant
        )
        # ── Nettoyage final ──────────────────────────────────────────────────
        base = (
            base
            # Filtre les années avant 1950 (données peu fiables) et les stocks nuls
            .loc[lambda x: (x["annee"] >= 1950) & (x["nb_equipements"] > 0)]
            .drop(columns="delta")
            .rename(columns={
                "dep_code": "Code_departement",
                "nb_equipements": "[equipement_sportif]nb_equipements"
            })
            .sort_values(["annee", "Code_departement"], kind="mergesort")
            .reset_index(drop=True)
        )
        return base
    except Exception as e:
        logger.error(f"clean_equipement_sportif() : erreur → {e}")
        raise

def clean_professionnels_sante(dfs: dict) -> pd.DataFrame:
    """
    Consolide les données de revenus fiscaux moyens par département
    sur une longue période (1984–2023), à partir de plusieurs fichiers
    Excel de formats hétérogènes (DGFiP).

    Structure attendue du dictionnaire `dfs` :
        - dfs["8420"] : dict de DataFrames clés '1984_1999', '2000_2017', '2018', '2019_2020'
        - dfs["21"]   : dict avec clé 'Feuil1' (données 2021)
        - dfs["22"]   : dict avec clé 'Feuil1' (données 2022)
        - dfs["23"]   : dict avec clé 'ListeCommune' (données 2023)

    Particularité 2021-2023 :
        - Les données sont au niveau commune, agrégées par département.
        - Les revenus sont en milliers d'euros (multiplication par 1000 nécessaire).

    Colonne produite :
        - [revenu_moyen]revenu_moyen_par_foyer : revenu fiscal de référence / nb foyers

    Args:
        dfs (dict): Dictionnaire structuré de DataFrames par source/période.

    Returns:
        pd.DataFrame: Revenus moyens par département et par année, 1984–2023.
    """
    try:
        dfs_annees = []
        for annee, dict_df in dfs.items():
            df_temp_annee = {}
            for source, df in dict_df.items():
                if source != "Lisez moi" and source != "Nomenclature des PS" and source != "Psychologues":
                    #Selectionne les colone voulu
                    df = df[[
                        "DEPARTEMENT",
                        "EFFECTIF",
                        "DENSITE /100 000 hab."
                    ]].copy()
                    # extraire uniquement le numéro du département
                    df["DEPARTEMENT"] = df["DEPARTEMENT"].str.extract(r'^([0-9A-Z]+)')
                    # supprimer les lignes TOTAL si elles existent
                    df = df[~df["DEPARTEMENT"].str.contains("TOTAL", na=False)]
                    # somme par département
                    df = df.groupby("DEPARTEMENT")[["EFFECTIF", "DENSITE /100 000 hab."]].sum().reset_index()
                    # Renomme les colones
                    df = df.rename(columns={"EFFECTIF": f"[{source}]EFFECTIF"})
                    df = df.rename(columns={"DENSITE /100 000 hab.": f"[{source}]DENSITE /100 000 hab."})
                    df_temp_annee[source] = df
            # fusion des sources
            df_final_annee = reduce(
                lambda left, right: pd.merge(left, right, on="DEPARTEMENT", how="outer"),
                df_temp_annee.values()
            )
            # ajouter l'année
            df_final_annee["annee"] = annee
            dfs_annees.append(df_final_annee)
        # concaténer toutes les années
        df_final = pd.concat(dfs_annees, ignore_index=True)
        # ordre des colonnes
        cols = ["DEPARTEMENT", "annee"] + [c for c in df_final.columns if c not in ["annee","DEPARTEMENT"]]
        df_final = df_final[cols]
        # Renomme la colone departement
        df_final = df_final.rename(columns={"DEPARTEMENT": "Code_departement"})
        df_final["annee"] = pd.to_numeric(df_final["annee"], errors="coerce").astype("int64")
        return df_final
    except KeyError as e:
        logger.error(f"clean_revenu_moyen() : clé manquante dans dfs → {e}")
        raise
    except Exception as e:
        logger.error(f"clean_revenu_moyen() : erreur → {e}")
        raise

def clean_etablissement_culturel(df: pd.DataFrame, df_2024: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données sur les établissements culturels par département et année.

    Ce fichier contient déjà les données agrégées — le nettoyage consiste
    principalement à supprimer les colonnes calculées redondantes et à
    normaliser les noms de colonnes.

    Colonnes supprimées :
        - pct_culturel             : pourcentage calculé, recalculable
        - nombre_etablissements    : doublon de la colonne renommée
        - libelle_geographique     : libellé textuel, la clé suffit

    Colonnes produites :
        - Code_departement
        - annee
        - [etablissement_culturel]nombre_etablissements

    Args:
        df (pd.DataFrame): DataFrame brut des établissements culturels.,
        df_2024 (pd.DataFrame): DataFrame brut des établissements culturels de 2025.

    Returns:
        pd.DataFrame: Données nettoyées et renommées.

    Raises:
        ValueError: Si le DataFrame résultant a moins de 3 colonnes.
    """
    try:
        # Suppression des colonnes redondantes ou inutiles
        df = df.drop(
            columns=[
                "pct_culturel",
                "nombre_etablissements",
                "libelle_geographique"
            ],
            errors="ignore"
        )
        # Vérification qu'il reste bien 3 colonnes (dept, année, nombre)
        if len(df.columns) < 3:
            raise ValueError(
                f"Le DataFrame ne contient pas les colonnes attendues "
                f"(seulement {len(df.columns)} colonnes après nettoyage)."
            )
        # Renommage basé sur la position (robuste aux changements de noms sources)
        df = df.rename(columns={
            df.columns[0]: "annee",
            df.columns[1]: "Code_departement",
            df.columns[2]: "[etablissement_culturel]nombre_etablissements"
        })
        # Réorganisation dans l'ordre standard (dept, année, valeur)
        df = df.loc[:, [
            "Code_departement",
            "annee",
            "[etablissement_culturel]nombre_etablissements"
        ]]
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("int64")
        df["[etablissement_culturel]nombre_etablissements"] = pd.to_numeric(df["[etablissement_culturel]nombre_etablissements"], errors="coerce").astype("int64")
        
        #On conserve que les Departement
        df_2024 = df_2024[df_2024["GEO_OBJECT"] == "DEP"].copy()
        #On conserve que les Departement
        df_2024 = df_2024[df_2024["TIME_PERIOD"] == 2024].copy()
        # Suppression des colonnes redondantes ou inutiles
        df_2024 = df_2024.drop(
            columns=[
                "GEO_OBJECT",
                "FACILITY_SDOM",
                "FACILITY_TYPE",
                "BPE_MEASURE",
                "INDOOR",
                "LIGHTED",
                "ERP_CATEGORY",
                "PRACTICE_AREA_ACCESSIBILITY",
                "SEASONAL_OPENING",
                "MULTIPLEX_CINEMA",
                "FACILITY_DOM",
                "SANITARY_ACCESSIBILITY",
                "LOCKER_ROOM_ACCESSIBILITY",
                "FREE_ACCESS",
                "SHOWER",
                "SANITARY"
            ],
            errors="ignore"
        )
        # Renommage basé sur position
        df_2024 = df_2024.rename(columns={
            df_2024.columns[0]: "Code_departement",
            df_2024.columns[1]: "annee",
            df_2024.columns[2]: "[etablissement_culturel]nombre_etablissements"
        })
        # Toutes les valeurs d’un même département et même année sont additionnées.
        df_2024 = (
            df_2024.groupby(["Code_departement", "annee"], as_index=False)["[etablissement_culturel]nombre_etablissements"]
            .sum()
        )
        # Normalisation des types de données
        df["Code_departement"] = df["Code_departement"].astype("string")
        df_2024["Code_departement"] = df_2024["Code_departement"].astype("string")
        # Concaténer les DataFrames
        df_final = pd.concat([df, df_2024], ignore_index=True)
        return df_final.reset_index(drop=True)
    except ValueError:
        raise
    except Exception as e:
        logger.error(f"clean_etablissement_culturel() : erreur → {e}")
        raise

def clean_pouvoir_achat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données nationales de pouvoir d'achat du revenu disponible brut
    (variation annuelle en %).

    Problème d'entrée :
        - Fichier Excel avec 2 lignes de titre en tête
        - Virgule comme séparateur décimal
        - Lignes non numériques (titres, notes) en fin de fichier

    Colonnes produites :
        - annee
        - [pouvoir_achat]pourcentage_annee_precedente : variation % vs N-1

    Args:
        df (pd.DataFrame): DataFrame brut issu du fichier INSEE pouvoir d'achat.

    Returns:
        pd.DataFrame: Série temporelle des variations de pouvoir d'achat.

    Raises:
        ValueError: Si le DataFrame a moins de 2 colonnes après nettoyage.
    """
    try:
        # Suppression des 2 premières lignes de titre
        df = df.iloc[2:].copy()
        # La première ligne utile devient l'en-tête
        df.columns = df.iloc[0]
        df = df.iloc[2:].reset_index(drop=True)
        df.columns.name = None
        # Suppression des lignes entièrement vides
        df = df.dropna(how="all")
        # Filtrage des lignes non numériques en fin de fichier (notes, sources)
        df = df.iloc[:-6].copy()
        # Calcul annuel
        df["Pouvoir d'achat du RDB"] = (
            df["Pouvoir d'achat du RDB"]
            .astype(str)              # force en string
            .str.replace(",", ".", regex=False)  # remplace virgule par point
            .str.strip()              # enlève espaces
        )
        df["Pouvoir d'achat du RDB"] = pd.to_numeric(df["Pouvoir d'achat du RDB"], errors="coerce")
        df["Revenu disponible brut (RDB)"] = (
            df["Revenu disponible brut (RDB)"]
            .astype(str)              # force en string
            .str.replace(",", ".", regex=False)  # remplace virgule par point
            .str.strip()              # enlève espaces
        )
        df["Revenu disponible brut (RDB)"] = pd.to_numeric(df["Revenu disponible brut (RDB)"], errors="coerce")
        df["annee"] = df["Trimestre"].str[:4]
        df = df.groupby("annee").agg({
            "Pouvoir d'achat du RDB": "mean",
            "Revenu disponible brut (RDB)": "mean"
        }).reset_index()
        # Renommage basé sur position
        df = df.rename(columns={
            df.columns[0]: "annee",
            df.columns[1]: "[pouvoir_achat]Pouvoir d'achat du RDB",
            df.columns[2]: "[pouvoir_achat]Revenu disponible brut (RDB)"
        })
        # Conversion numérique propre
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce")
        return df.reset_index(drop=True)
    except ValueError:
        raise
    except Exception as e:
        logger.error(f"clean_pouvoir_achat() : erreur → {e}")
        raise

def clean_niveau_etude(df: pd.DataFrame, df_2024: pd.DataFrame, metadata_niveau_etude: str) -> pd.DataFrame:
    """
    Nettoie et pivote les données de niveau d'étude (diplômes) par département
    et par année, issues de l'INSEE (format SDMX).

    Harmonisation des codes diplôme :
        - '001T100_RP' et '001T200_RP' sont normalisés en '001T003_RP'
          (agrégats redondants, ramenés à un code unique).

    Décalage temporel :
        L'année est décalée de -1 pour alignement sur l'année électorale.

    Colonnes produites :
        - Code_departement
        - annee
        - [niveau_etude]<libellé diplôme> : une colonne par diplôme

    Args:
        df (pd.DataFrame): Données brutes INSEE format SDMX.
        metadata_niveau_etude (str): Chemin vers le JSON code diplôme → libellé.

    Returns:
        pd.DataFrame: Données pivotées par département, année et diplôme.
    """
    try:
        # Suppression des colonnes de métadonnées inutiles
        df = df.drop(
            columns=[
                'STUD_AREA', 'SEX', 'FREQ',
                'RP_MEASURE', 'AGE', 'OBS_STATUS'
            ],
            errors="ignore"
        )
        # Harmonisation des codes : deux codes agrégats → code unique
        # Évite les doublons lors du pivot
        df.loc[
            df["EDUC"].isin(["001T100_RP", "001T200_RP"]),
            "EDUC"
        ] = "001T003_RP"
        # Chargement du mapping code diplôme → libellé
        with open(metadata_niveau_etude, 'r', encoding='utf-8') as f:
            mapping = {
                normaliser(item['code']): item['libelle']
                for item in json.load(f)
            }
        # Application du mapping
        df["EDUC"] = (
            df["EDUC"]
            .astype(str)
            .apply(normaliser)
            .map(mapping)
        )
        # Extraction du code département et décalage temporel
        df["Code_departement"] = df["GEO"].str.split("-").str[-1]
        df["annee"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce") + 1 # Décalage de +1 pour alignement sur année électorale
        df.loc[df["annee"] == 2023, "annee"] = 2022
        df = df.drop(columns=["GEO", "TIME_PERIOD"], errors="ignore")
        # Renommage des colonnes analytiques
        df = df.rename(columns={
            "EDUC": "[niveau_etude]diplome",
            "OBS_VALUE_NIVEAU": "[niveau_etude]nombre_diplome"
        })
        # Sélection et réorganisation
        df = df[[
            "Code_departement",
            "annee",
            "[niveau_etude]diplome",
            "[niveau_etude]nombre_diplome"
        ]]
        # Pivot : une colonne par diplôme
        df = (
            df.pivot_table(
                index=["annee"],
                columns="[niveau_etude]diplome",
                values="[niveau_etude]nombre_diplome",
                aggfunc="sum"
            )
            .fillna(0)  # 0 diplômés si absent (pas NaN)
        )
        df.columns.name = None
        # Ajout du préfixe source sur chaque colonne diplôme
        df = df.rename(columns=lambda x: f"[niveau_etude]{x}")
        df = df.rename(columns={"[niveau_etude]Baccalauréat universitaire ou équivalent : Licence, licence pro, maîtrise, diplôme équivalent de niveau bac+3 ou bac+4": "[niveau_etude]Baccalauréat universitaire ou équivalent"})
        df = df.rename(columns={"[niveau_etude]Enseignement supérieur de cycle court : BTS, DUT, Deug, Deust, diplôme de la santé ou du social de niveau bac+2, diplôme": "[niveau_etude]Enseignement supérieur de cycle court"})
        # Harmonisation des nom pour le merge
        df = df.rename(columns={"[niveau_etude]BEPC, brevet élémentaire, brevet des collèges, DNB": "[niveau_etude]Brevet des collèges"})
        df = df.rename(columns={"[niveau_etude]CAP, BEP ou diplôme de niveau équivalent": "[niveau_etude]CAP, BEP ou équivalent"})
        df = df.rename(columns={"[niveau_etude]Enseignement supérieur de cycle court": "[niveau_etude]Diplôme de niveau bac+2"})
        df = df.rename(columns={"[niveau_etude]Diplôme universitaire 2e ou 3e cycle": "[niveau_etude]Diplôme de niveau bac+3 ou bac+4"})
        df["[niveau_etude]Aucun diplôme, CEP"] = df["[niveau_etude]Aucun diplôme"] + df["[niveau_etude]CEP (certificat d’études primaires)"]
        df = df.drop(columns=["[niveau_etude]Aucun diplôme", "[niveau_etude]CEP (certificat d’études primaires)"])
        df["[niveau_etude]Baccalauréat ou équivalent"] = df["[niveau_etude]Baccalauréat, brevet professionnel ou équivalent"] + df["[niveau_etude]Baccalauréat universitaire ou équivalent"]
        df = df.drop(columns=["[niveau_etude]Baccalauréat, brevet professionnel ou équivalent", "[niveau_etude]Baccalauréat universitaire ou équivalent"])
        df["[niveau_etude]Diplôme de niveau bac+5 ou plus"] = df["[niveau_etude]Diplôme de niveau bac + 5 ou plus"] + df["[niveau_etude]Diplôme d'études supérieures"]
        df = df.drop(columns=["[niveau_etude]Diplôme de niveau bac + 5 ou plus", "[niveau_etude]Diplôme d'études supérieures"])
        df = df.groupby("annee").sum()
        df = df.reset_index(drop=False)

        # Suppression des 2 premières lignes de titre
        df_2024 = df_2024.iloc[2:].copy()
        # La première ligne utile devient l'en-tête
        df_2024.columns = df_2024.iloc[0]
        df_2024 = df_2024.iloc[1:].reset_index(drop=True)
        df_2024.columns.name = None
        # Suppression des lignes entièrement vides
        df_2024 = df_2024.dropna(how="all")
        # Filtrage des lignes non numériques en fin de fichier (notes, sources)
        df_2024 = df_2024.iloc[:-6].copy()
        #Nettoyer les noms de colonnes
        df_2024.columns = df_2024.columns.str.strip()
        #un seul pourcentage global (Femmes + Hommes)
        df_2024["total"] = df_2024[[
            "Femmes","Hommes"
        ]].mean(axis=1)
        # Suppression des colonnes inutiles
        df_2024 = df_2024.drop(
            columns=[
                'Femmes', 'Hommes'
            ],
            errors="ignore"
        )
        #pivot la table
        df_2024 = df_2024.rename(columns={df_2024.columns[0]: "niveau_diplome"})
        df_2024 = df_2024.set_index("niveau_diplome").T.reset_index(drop=True)
        df_2024.insert(0, "annee", 2024)
        df_2024.columns.name = None
        # Suppression des colonnes inutiles
        df_2024 = df_2024.drop(
            columns=[
                "niveau_diplome"
            ],
            errors="ignore"
        )
        # Harmonisation des nom pour le merge
        df_2024 = df_2024.rename(columns={"Aucun diplôme, certificat d’études primaires": "[niveau_etude]Aucun diplôme, CEP"})
        df_2024 = df_2024.rename(columns={"Brevet des collèges              ": "[niveau_etude]Brevet des collèges"})
        df_2024 = df_2024.rename(columns={"CAP, BEP ou équivalent": "[niveau_etude]CAP, BEP ou équivalent"})
        df_2024 = df_2024.rename(columns={"Baccalauréat ou équivalent": "[niveau_etude]Baccalauréat ou équivalent"})
        df_2024 = df_2024.rename(columns={"Diplôme de niveau bac+5 ou plus       ": "[niveau_etude]Diplôme de niveau bac+5 ou plus"})
        df_2024 = df_2024.rename(columns={"Diplôme de niveau bac+2": "[niveau_etude]Diplôme de niveau bac+2"})
        df_2024 = df_2024.rename(columns={"Diplôme de niveau bac+3 ou bac+4   ": "[niveau_etude]Diplôme de niveau bac+3 ou bac+4"})
        # calcul du total par ligne (sans la colonne année)
        df_pct = df.copy()
        totaux = df.iloc[:, 1:].sum(axis=1)
        # conversion en pourcentage
        df_pct.iloc[:, 1:] = df.iloc[:, 1:].div(totaux, axis=0) * 100
        df = df_pct
        # Concaténer les DataFrames
        df_final = pd.concat([df, df_2024], ignore_index=True)
        return df_final.reset_index()
    except FileNotFoundError:
        logger.error(f"clean_niveau_etude() : JSON introuvable → {metadata_niveau_etude}")
        raise
    except Exception as e:
        logger.error(f"clean_niveau_etude() : erreur → {e}")
        raise