import pandas as pd
import json
import unicodedata

def normaliser(texte):
    """Normalise le texte : minuscules + suppression des accents"""
    texte = texte.lower()
    # Supprime les accents
    texte = unicodedata.normalize('NFD', texte)
    texte = ''.join(char for char in texte if unicodedata.category(char) != 'Mn')
    return texte
def clean_excel_block(df: pd.DataFrame, skip_rows: int, drop_last: int = 0):
    """Nettoyage générique Excel mal formé"""
    df = df.iloc[skip_rows:]
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df.columns.name = None
    if drop_last:
        df = df.iloc[:-drop_last]
    return df
def load_json_mapping(path: str, key_field: str, value_field: str, normalize=True) -> dict:
    """Charge un JSON et retourne un dictionnaire mapping optimisé"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if normalize:
        return {
            normaliser(item[key_field]): item[value_field]
            for item in data
        }
    return {item[key_field]: item[value_field] for item in data}
def fix_departement(code):
    code = str(code).strip()
    if code in ('2A0', '2B0'):
        return code[:-1]
    try:
        if int(code) >= 970:
            return code
        return str(int(code) // 10).zfill(2)
    except ValueError:
        return code
def _parse_numeric_col(series):
    return pd.to_numeric(
        series.astype(str).str.replace(" ", "").str.replace(",", "."),
        errors="coerce"
    )
def _set_header(df, skip_rows):
    df = df.iloc[skip_rows:]
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df.columns.name = None
    return df

def clean_delinquance(df: pd.DataFrame) -> pd.DataFrame: 
    required_cols = {"Code_departement", "annee", "nombre", "taux_pour_mille"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Colonnes manquantes : {missing}")

    return (
        df
        .groupby(
            ["Code_departement", "annee"],
            as_index=False,
            sort=False,
            observed=True
        )
        .agg(
            **{
                "[delinquance]nombre": ("nombre", "sum"),
                "[delinquance]taux_pour_mille": ("taux_pour_mille", "mean"),
            }
        )
    )

def clean_taux_chomage(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage Excel
    df = clean_excel_block(df, skip_rows=2, drop_last=4)
    # Suppression colonne inutile
    df = df.drop(columns="Libellé", errors="ignore")
    # Transformation wide → long
    df = df.melt(
        id_vars="Code",
        var_name="Periode",
        value_name="Taux"
    )
    # Normalisation
    df["Code"] = df["Code"].astype(str).str.zfill(2)
    df["annee"] = df["Periode"].str[-4:].astype(int)
    # Sécurise type numérique (important si Excel)
    df["Taux"] = pd.to_numeric(df["Taux"], errors="coerce")
    # Agrégation annuelle
    df = (
        df
        .groupby(["Code", "annee"], as_index=False, sort=False)
        .agg(**{"[taux_chomage]Taux_moyen": ("Taux", "mean")})
    )
    return df.rename(columns={"Code": "Code_departement"})

def clean_age_moyen(df: pd.DataFrame) -> pd.DataFrame:
    # Suppression colonnes inutiles
    df = df.drop(columns=["RP_MEASURE", "PCS", "SEX"], errors="ignore")
    # Extraction code département
    df["Code_departement"] = df["GEO"].str.rsplit("-", n=1).str[-1]
    # Gestion tri correct avec Corse
    sort_series = (
        df["Code_departement"]
        .replace({"2A": "1000", "2B": "1001"})
        .astype(int)
    )
    df = (
        df
        .assign(_sort=sort_series)
        .sort_values("_sort", kind="mergesort")  # stable
        .drop(columns=["_sort", "GEO"])
    )
    # Renommage année
    df = df.rename(columns={"TIME_PERIOD": "annee"})
    # Pivot sécurisé
    df = (
        df
        .pivot_table(
            index=["Code_departement", "annee"],
            columns="AGE",
            values="OBS_VALUE_NIVEAU",
            aggfunc="first"  # plus rapide que sum si unique
        )
        .reset_index()
    )
    df.columns.name = None
    # Drop + renommage en une seule étape
    return df.rename(columns={
        "Y15T24": "[age_moyen]entre15et24",
        "Y25T54": "[age_moyen]entre25et54",
        "Y_GE55": "[age_moyen]plus55"
    }).drop(columns=["Y_GE15"], errors="ignore")

def clean_president_sortant(df: pd.DataFrame, metadata_famille_politique: str) -> pd.DataFrame:
    # Filtre présidentielles T1 / T2 + extraction année/tour
    mask = df["id_election"].str.contains("pres_t", na=False)
    df = df.loc[mask].copy()
    df[["annee", "tour"]] = df["id_election"].str.extract(
        r"(\d{4})_pres_(t[12])",
        expand=True
    )
    df["annee"] = df["annee"].astype(int) - 1
    # Suppression colonnes inutiles
    df = df.drop(
        columns=[
            "id_election", "id_brut_miom", "code_commune", "code_bv",
            "nuance", "sexe", "no_panneau",
            "ratio_voix_inscrits", "ratio_voix_exprimes",
            "libelle_abrege_liste", "nom_tete_liste",
            "binome", "liste", "libelle_etendu_liste", "voix"
        ],
        errors="ignore"
    )
    # Sélection utile
    df = df[["code_departement", "annee", "tour", "nom", "prenom"]]
    # Suppression doublons AVANT concat (plus rapide)
    df = df.drop_duplicates(ignore_index=True)
    # Fusion nom/prénom (vectorisé)
    df["candidat"] = df["nom"].str.cat(df["prenom"], sep=" ")
    df = df.drop(columns=["nom", "prenom"])
    # Mapping famille politique (optimisé)
    mapping = load_json_mapping(
        metadata_famille_politique,
        key_field="nom",
        value_field="famille_politique",
        normalize=True
    )
    df["famille_politique"] = (
        df["candidat"]
        .astype(str)
        .apply(normaliser)
        .map(mapping)
    )
    # Renommage groupé
    df = df.rename(columns={
        "tour": "[president_sortant]tour",
        "candidat": "[president_sortant]candidat",
        "famille_politique": "[president_sortant]famille_politique"
    })
    df["code_departement"] = df["code_departement"].replace({
        "ZA": "971",
        "ZB": "972",
        "ZC": "973",
        "ZD": "974",
        "ZM": "976",
        "ZN": "988",
        "ZP": "987",
        "ZS": "975",
        "ZT": "978",
        "ZW": "986",
        "ZX": "977",
        "ZY": "977",
    })
    df = df[df.iloc[:, 0] != "ZZ"]
    # Tri final optimisé
    df = df.sort_values(
        ["code_departement", "annee"],
        kind="mergesort"
    ).reset_index(drop=True)
    return df

def clean_population_active(df: pd.DataFrame, metadata_population_active: str) -> pd.DataFrame:
    # Extraction code département + année
    df["Code_departement"] = df["GEO"].str.rsplit("-", n=1).str[-1]
    df["annee"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce") - 1
    # Suppression colonnes inutiles
    df = df.drop(
        columns=["SEX", "FREQ", "RP_MEASURE", "GEO", "TIME_PERIOD", "EDUC"],
        errors="ignore"
    )
    # Tri optimisé avec gestion Corse
    sort_series = (
        df["Code_departement"]
        .replace({"2A": "1000", "2B": "1001"})
        .astype(int)
    )
    df = (
        df
        .assign(_sort=sort_series)
        .sort_values("_sort", kind="mergesort")
        .drop(columns="_sort")
    )
    # Pivot sécurisé
    df = (
        df
        .pivot_table(
            index=["Code_departement", "annee", "EMPSTA_ENQ"],
            columns="AGE",
            values="OBS_VALUE_NIVEAU",
            aggfunc="first"   # plus rapide si pas de doublons
        )
        .reset_index()
    )
    df.columns.name = None
    # Drop + renommage groupé
    df = df.rename(columns={
        "Y15T24": "[population_active]entre15et24",
        "Y25T54": "[population_active]entre25et54",
        "Y55T64": "[population_active]entre55et64"
    }).drop(columns=["Y15T64", "Y_GE15"], errors="ignore")
    # Mapping statut emploi (optimisé)
    mapping = load_json_mapping(
        metadata_population_active,
        key_field="EMPSTA_ENQ",
        value_field="Statut_emploi",
        normalize=True
    )
    df["Statut_emploi"] = (
        df["EMPSTA_ENQ"]
        .astype(str)
        .apply(normaliser)
        .map(mapping)
    )
    # Réorganisation finale
    df = (
        df
        .drop(columns="EMPSTA_ENQ")
        .loc[:, [
            "Code_departement",
            "annee",
            "Statut_emploi",
            "[population_active]entre15et24",
            "[population_active]entre25et54",
            "[population_active]entre55et64",
        ]]
        .fillna(0)
        .reset_index(drop=True)
    )
    return df

def clean_categorie_professionnelle(df: pd.DataFrame, metadata_categorie_professionnelle: str) -> pd.DataFrame:
    # Suppression colonnes inutiles
    df = df.drop(
        columns=[
            'IMMI', 'EEC_MEASURE', 'SEX', 'EDUC', 'UNDEREMP',
            'EMPFORM', 'UNEMPDUR', 'COMPOHALO', 'EMPSTA',
            'WKTIME', 'ACTIVITY', 'AGE', 'OBS_STATUS',
            'UNIT_MULT', 'UNIT_MEASURE'
        ],
        errors="ignore"
    )
    # Conversion année + tri
    df["TIME_PERIOD"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce")
    df = df.sort_values("TIME_PERIOD", kind="mergesort")
    # Suppression des totaux (_T)
    df = df[~df["PCS"].str.contains("_T", na=False)]
    # Chargement mapping JSON
    with open(metadata_categorie_professionnelle, "r", encoding="utf-8") as f:
        mapping = {
            normaliser(item["code"]): item["libelle"]
            for item in json.load(f)
        }
    # Application mapping
    df["PCS"] = (
        df["PCS"]
        .astype(str)
        .apply(normaliser)
        .map(mapping)
    )
    # Pivot
    df = (
        df
        .pivot_table(
            index="TIME_PERIOD",
            columns="PCS",
            values="OBS_VALUE_NIVEAU",
            aggfunc="sum"
        )
        .reset_index()
    )
    df.columns.name = None
    # Renommage final
    df = df.rename(columns={"TIME_PERIOD": "annee"})
    df.columns = [
        col if col == "annee"
        else f"[categorie_professionnelle] {col}"
        for col in df.columns
    ]
    return df.reset_index(drop=True)

def clean_equipement_sportif(df: pd.DataFrame) -> pd.DataFrame:
    # Colonnes utiles uniquement
    df = df[[
        "dep_code",
        "equip_service_date",
        "equip_service_periode",
        "inst_hs_bool",
        "inst_date_etat"
    ]].copy()
    # Détermination année mise en service
    df["equip_service_date"] = pd.to_datetime(df["equip_service_date"], errors="coerce")
    annee_date = df["equip_service_date"].dt.year
    annees = (
        df["equip_service_periode"]
        .str.extract(r"(\d{4})(?:-(\d{4}))?")
        .astype(float)
    )
    annee_periode = annees[1].fillna(annees[0])
    df["equip_service_annee"] = (
        annee_date.fillna(annee_periode)
        .astype("Int64")
    )
    # Détermination année fin de service
    annee_actuelle = pd.Timestamp.now().year
    df["inst_date_etat"] = pd.to_datetime(df["inst_date_etat"], errors="coerce")
    annee_etat = df["inst_date_etat"].dt.year
    df["dern_inst_date"] = annee_actuelle
    df.loc[df["inst_hs_bool"] == True, "dern_inst_date"] = annee_etat
    df["dern_inst_date"] = df["dern_inst_date"].astype("Int64")
    # Construction des deltas
    df = df[["dep_code", "equip_service_annee", "dern_inst_date"]]
    debut = (
        df.groupby(["dep_code", "equip_service_annee"])
        .size()
        .reset_index(name="delta")
        .rename(columns={"equip_service_annee": "annee"})
    )
    fin = (
        df.groupby(["dep_code", "dern_inst_date"])
        .size()
        .reset_index(name="delta")
        .rename(columns={"dern_inst_date": "annee"})
    )
    fin["annee"] += 1
    fin["delta"] *= -1
    variations = pd.concat([debut, fin], ignore_index=True)
    # Construction grille complète
    annee_min = df["equip_service_annee"].min()
    annee_max = df["dern_inst_date"].max()
    base = (
        pd.MultiIndex.from_product(
            [df["dep_code"].unique(), range(annee_min, annee_max + 1)],
            names=["dep_code", "annee"]
        )
        .to_frame(index=False)
        .merge(variations, on=["dep_code", "annee"], how="left")
    )
    base["delta"] = base["delta"].fillna(0)
    # Cumul équipements actifs
    base["nb_equipements"] = (
        base.groupby("dep_code")["delta"]
        .cumsum()
    )
    # Nettoyage final
    base = (
        base
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

def clean_revenu_moyen(dfs: dict) -> pd.DataFrame:
    # ── 8420 ──────────────────────────────────────────────────────────────────
    dfs["8420"] = {k: v for k, v in dfs["8420"].items() if k.lower() != "notice"}

    # 1984_1999
    df = _set_header(dfs["8420"]["1984_1999"], skip_rows=7)
    df = df.iloc[:, 1:].iloc[:, :-5].drop(columns=["Nom"])
    df["[revenu_moyen]revenu_moyen_par_foyer"] = (
        pd.to_numeric(df["Revenus nets imposables"], errors="coerce") /
        pd.to_numeric(df["Nombre de foyers fiscaux"], errors="coerce")
    )
    df = df.drop(columns=["Revenus nets imposables", "Nombre de foyers fiscaux"])
    # At this point columns are: [col0_dept, col1_annee, ..., revenu_moyen]
    # Rename the first two positionally, keep the rest
    df.columns = ["Code_departement", "annee"] + list(df.columns[2:])
    df["Code_departement"] = df["Code_departement"].apply(fix_departement)
    dfs["8420"]["1984_1999"] = df[["Code_departement", "annee", "[revenu_moyen]revenu_moyen_par_foyer"]]

    # 2000_2017
    df = _set_header(dfs["8420"]["2000_2017"], skip_rows=7)
    df = df.iloc[:, 1:].iloc[:, :-7].drop(columns=["Nom"])
    df["[revenu_moyen]revenu_moyen_par_foyer"] = (
        pd.to_numeric(df["Revenu fiscal de référence"], errors="coerce") /
        pd.to_numeric(df["Nombre de foyers fiscaux"], errors="coerce")
    )
    df = df.drop(columns=["Revenu fiscal de référence", "Nombre de foyers fiscaux"])
    df.columns = ["Code_departement", "annee"] + list(df.columns[2:])  # ← rename FIRST
    df = df[df["Code_departement"] != "B31"]                            # ← filter AFTER
    df["Code_departement"] = df["Code_departement"].apply(fix_departement)
    dfs["8420"]["2000_2017"] = df[["Code_departement", "annee", "[revenu_moyen]revenu_moyen_par_foyer"]]

    # 2018
    df = _set_header(dfs["8420"]["2018"], skip_rows=7)
    df.drop(df.tail(4).index, inplace=True)
    df = df.iloc[:, 1:].iloc[:, :-9].drop(columns=["Nom"])
    df["[revenu_moyen]revenu_moyen_par_foyer"] = (
        pd.to_numeric(df["Revenu fiscal de référence"], errors="coerce") /
        pd.to_numeric(df["Nombre de foyers fiscaux"], errors="coerce")
    )
    df = df.drop(columns=["Revenu fiscal de référence", "Nombre de foyers fiscaux"])
    df.columns = ["Code_departement", "annee"] + list(df.columns[2:])  # ← rename FIRST
    df = df[df["Code_departement"] != "B31"]                            # ← filter AFTER
    df["Code_departement"] = df["Code_departement"].apply(fix_departement)
    dfs["8420"]["2018"] = df[["Code_departement", "annee", "[revenu_moyen]revenu_moyen_par_foyer"]]

    # 2019_2020
    df = _set_header(dfs["8420"]["2019_2020"], skip_rows=7)
    df = df.iloc[:, 1:].iloc[:, :-7].drop(columns=["Nom"])
    df["[revenu_moyen]revenu_moyen_par_foyer"] = (
        pd.to_numeric(df["Revenu fiscal de référence"], errors="coerce") /
        pd.to_numeric(df["Nombre de foyers fiscaux"], errors="coerce")
    )
    df = df.drop(columns=["Revenu fiscal de référence", "Nombre de foyers fiscaux"])
    df.columns = ["Code_departement", "annee"] + list(df.columns[2:])  # ← rename FIRST
    df = df[df["Code_departement"] != "B31"]                            # ← filter AFTER
    df["Code_departement"] = df["Code_departement"].apply(fix_departement)
    dfs["8420"]["2019_2020"] = df[["Code_departement", "annee", "[revenu_moyen]revenu_moyen_par_foyer"]]

    # ── 2021 ──────────────────────────────────────────────────────────────────
    df = _set_header(dfs["21"]["Feuil1"], skip_rows=6)
    df = df.iloc[:, 1:].iloc[:, :-7]
    df = df[df.iloc[:, 3].astype(str).str.strip().str.lower() == "total"]
    df = df[df.iloc[:, 0] != "B31"]
    df.columns = ["Code_departement", "commune", "libelle_commune", "tranche", "nbr_foyer", "revenue_referance"]
    df["nbr_foyer"] = _parse_numeric_col(df["nbr_foyer"])
    df["revenue_referance"] = _parse_numeric_col(df["revenue_referance"])
    df["[revenu_moyen]revenu_moyen_par_foyer"] = df["revenue_referance"] * 1000 / df["nbr_foyer"]
    df["Code_departement"] = df["Code_departement"].apply(fix_departement)
    df = df.groupby("Code_departement", as_index=False)["[revenu_moyen]revenu_moyen_par_foyer"].mean()
    df["annee"] = 2021
    dfs["21"] = df[["Code_departement", "annee", "[revenu_moyen]revenu_moyen_par_foyer"]]

    # ── 2022 ──────────────────────────────────────────────────────────────────
    df = dfs["22"]["Feuil1"]
    df = df.iloc[4:]
    df.columns = df.iloc[0]
    df = df.iloc[2:].reset_index(drop=True)
    df.drop(df.tail(2).index, inplace=True)
    df.columns.name = None
    df = df.iloc[:, :-7]
    df = df[df.iloc[:, 3].astype(str).str.strip().str.lower() == "total"]
    df["Revenu fiscal de référence des foyers fiscaux"] = _parse_numeric_col(df["Revenu fiscal de référence des foyers fiscaux"])
    df["Nombre de foyers fiscaux"] = _parse_numeric_col(df["Nombre de foyers fiscaux"])
    df["[revenu_moyen]revenu_moyen_par_foyer"] = (
        df["Revenu fiscal de référence des foyers fiscaux"] * 1000 / df["Nombre de foyers fiscaux"]
    )
    df = df[df["Dép."] != "B31"]
    df["Code_departement"] = df["Dép."].apply(fix_departement)
    df["annee"] = 2022
    dfs["22"] = df.groupby("Code_departement", as_index=False)["[revenu_moyen]revenu_moyen_par_foyer"].mean()
    dfs["22"]["annee"] = 2022
    dfs["22"] = dfs["22"][["Code_departement", "annee", "[revenu_moyen]revenu_moyen_par_foyer"]]

    # ── 2023 ──────────────────────────────────────────────────────────────────
    df = dfs["23"]["ListeCommune"]
    df = df.iloc[4:]
    df.columns = df.iloc[0]
    df = df.iloc[2:].reset_index(drop=True)
    df.columns.name = None
    df = df.iloc[:, :-7]
    df = df[df.iloc[:, 3].astype(str).str.strip().str.lower() == "total"]
    df["Revenu fiscal de référence des foyers fiscaux"] = _parse_numeric_col(df["Revenu fiscal de référence des foyers fiscaux"])
    df["Nombre de foyers fiscaux"] = _parse_numeric_col(df["Nombre de foyers fiscaux"])
    df["[revenu_moyen]revenu_moyen_par_foyer"] = (
        df["Revenu fiscal de référence des foyers fiscaux"] * 1000 / df["Nombre de foyers fiscaux"]
    )
    df = df[df["Dép."] != "B31"]
    df["Code_departement"] = df["Dép."].apply(fix_departement)
    df = df.groupby("Code_departement", as_index=False)["[revenu_moyen]revenu_moyen_par_foyer"].mean()
    df["annee"] = 2023
    dfs["23"] = df[["Code_departement", "annee", "[revenu_moyen]revenu_moyen_par_foyer"]]

    # ── Concat final ──────────────────────────────────────────────────────────
    dfs["8420"] = pd.concat(dfs["8420"].values(), ignore_index=True)
    return pd.concat([dfs["8420"], dfs["21"], dfs["22"], dfs["23"]], ignore_index=True)

def clean_etablissement_culturel(df: pd.DataFrame) -> pd.DataFrame:
    # Suppression colonnes inutiles (sans crash si absentes)
    df = df.drop(
        columns=[
            "pct_culturel",
            "nombre_etablissements",
            "libelle_geographique"
        ],
        errors="ignore"
    )
    # Vérification nombre colonnes restantes
    if len(df.columns) < 3:
        raise ValueError("Le DataFrame ne contient pas les colonnes attendues.")
    # Renommage sécurisé basé sur position
    df = df.rename(columns={
        df.columns[0]: "annee",
        df.columns[1]: "Code_departement",
        df.columns[2]: "[etablissement_culturel]nombre_etablissements"
    })
    # Réorganisation propre
    df = df.loc[:, [
        "Code_departement",
        "annee",
        "[etablissement_culturel]nombre_etablissements"
    ]]
    return df.reset_index(drop=True)

def clean_pouvoir_achat(df: pd.DataFrame) -> pd.DataFrame:
    # Suppression des premières lignes inutiles
    df = df.iloc[2:].copy()
    # Définition des colonnes à partir de la première vraie ligne
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df.columns.name = None
    # Suppression des lignes totalement vides
    df = df.dropna(how="all")
    # Suppression des lignes non numériques en fin de fichier
    df = df[pd.to_numeric(df.iloc[:, 0], errors="coerce").notna()]
    # Suppression colonne inutile (sans crash)
    df = df.drop(
        columns=["Pouvoir d’achat du revenu disponible brut"],
        errors="ignore"
    )
    # Renommage sécurisé basé sur position
    if len(df.columns) < 2:
        raise ValueError("Format inattendu pour le fichier pouvoir_achat")
    df = df.rename(columns={
        df.columns[0]: "annee",
        df.columns[1]: "[pouvoir_achat]pourcentage_annee_precedente"
    })
    # Conversion numérique propre
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce")
    df["[pouvoir_achat]pourcentage_annee_precedente"] = (
        df["[pouvoir_achat]pourcentage_annee_precedente"]
        .astype(str)
        .str.replace(",", ".", regex=False)
    )
    df["[pouvoir_achat]pourcentage_annee_precedente"] = pd.to_numeric(
        df["[pouvoir_achat]pourcentage_annee_precedente"],
        errors="coerce"
    )
    return df.reset_index(drop=True)

def clean_niveau_etude(df: pd.DataFrame, metadata_niveau_etude: str) -> pd.DataFrame:
    # Suppression colonnes inutiles
    df = df.drop(
        columns=[
            'STUD_AREA', 'SEX', 'FREQ',
            'RP_MEASURE', 'AGE', 'OBS_STATUS'
        ],
        errors="ignore"
    )
    # Harmonisation codes diplômes
    df.loc[
        df["EDUC"].isin(["001T100_RP", "001T200_RP"]),
        "EDUC"
    ] = "001T003_RP"
    # Mapping diplôme
    with open(metadata_niveau_etude, 'r', encoding='utf-8') as f:
        mapping = {
            normaliser(item['code']): item['libelle']
            for item in json.load(f)
        }
    df["EDUC"] = (
        df["EDUC"]
        .astype(str)
        .apply(normaliser)
        .map(mapping)
    )
    # Normalisation département + année
    df["Code_departement"] = df["GEO"].str.split("-").str[-1]
    df["annee"] = (
        pd.to_numeric(df["TIME_PERIOD"], errors="coerce") - 1
    )
    df = df.drop(columns=["GEO", "TIME_PERIOD"], errors="ignore")
    # Renommage propre
    df = df.rename(columns={
        "EDUC": "[niveau_etude]diplome",
        "OBS_VALUE_NIVEAU": "[niveau_etude]nombre_diplome"
    })
    df = df[[
        "Code_departement",
        "annee",
        "[niveau_etude]diplome",
        "[niveau_etude]nombre_diplome"
    ]]
    # Pivot sécurisé
    df = (
        df.pivot_table(
            index=["Code_departement", "annee"],
            columns="[niveau_etude]diplome",
            values="[niveau_etude]nombre_diplome",
            aggfunc="sum"
        )
        .fillna(0)
    )
    df.columns.name = None
    # Ajout préfixe uniforme
    df = df.rename(columns=lambda x: f"[niveau_etude]{x}")
    return df.reset_index()

def clean_abstention_votant(df: pd.DataFrame) -> pd.DataFrame:
    # Filtre présidentielles
    df = df[
        df["id_election"]
        .astype(str)
        .str.contains(r"pres_t1|pres_t2", na=False)
    ].copy()
    # Extraction année + tour
    df[["annee", "tour"]] = df["id_election"].str.extract(
        r"(\d{4})_pres_(t[12])"
    )
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce") - 1
    df = df.drop(columns=["id_election"], errors="ignore")
    # Suppression colonnes inutiles
    df = df.drop(columns=[
        'id_brut_miom', 'code_commune', 'libelle_canton',
        'code_canton', 'libelle_departement',
        'code_circonscription', 'libelle_commune',
        'libelle_circonscription', 'code_bv',
        'ratio_blancs_votants', 'ratio_nuls_inscrits',
        'ratio_nuls_votants', 'ratio_exprimes_inscrits',
        'ratio_exprimes_votants', 'ratio_abstentions_inscrits',
        'ratio_votants_inscrits', 'ratio_blancs_inscrits',
        'votants', 'exprimes'
    ], errors="ignore")
    # Sélection colonnes utiles
    df = df[[
        "code_departement",
        "annee",
        "tour",
        "inscrits",
        "abstentions",
        "blancs",
        "nuls"
    ]]
    df = df.fillna(0)
    # Renommage
    df = df.rename(columns={
        "tour": "[abstention_votant]tour",
        "inscrits": "[abstention_votant]inscrits",
        "abstentions": "[abstention_votant]abstentions",
        "blancs": "[abstention_votant]blancs",
        "nuls": "[abstention_votant]nuls"
    })
    # Harmonisation DOM-TOM
    mapping_dom = {
        "ZA": "971", "ZB": "972", "ZC": "973",
        "ZD": "974", "ZM": "976", "ZN": "988",
        "ZP": "987", "ZS": "975", "ZT": "978",
        "ZW": "986", "ZX": "977", "ZY": "977",
    }
    df["code_departement"] = df["code_departement"].replace(mapping_dom)
    df = df[df["code_departement"] != "ZZ"]
    # Agrégation département
    df = (
        df.groupby(
            ["code_departement", "annee", "[abstention_votant]tour"],
            as_index=False
        )[[
            "[abstention_votant]inscrits",
            "[abstention_votant]abstentions",
            "[abstention_votant]blancs",
            "[abstention_votant]nuls"
        ]]
        .sum()
        .sort_values(["code_departement", "annee"])
        .reset_index(drop=True)
    )
    return df