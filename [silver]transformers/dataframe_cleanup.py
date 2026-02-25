import pandas as pd
import json
import unicodedata
import numpy as np

def normaliser(texte):
  """Normalise le texte : minuscules + suppression des accents"""
  texte = texte.lower()
  # Supprime les accents
  texte = unicodedata.normalize('NFD', texte)
  texte = ''.join(char for char in texte if unicodedata.category(char) != 'Mn')
  return texte

def clean_delinquance(df: pd.DataFrame) -> pd.DataFrame:
  """
  Agrège les données par département et année.
  - Somme de 'nombre'
  - Moyenne de 'taux_pour_mille'
  - Supprime les autres colonnes
  
  renomme les colonnes.
  - nombre: [delinquance]nombre
  - taux_pour_mille: [delinquance]taux_pour_mille

  Parameters
  ----------
  df : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """

  df_out = (
      df
      .groupby(["Code_departement", "annee"], as_index=False)
      .agg({
          "nombre": "sum",
          "taux_pour_mille": "mean"
      })
  )
  df_out = df_out.rename(columns={'nombre': '[delinquance]nombre', 'taux_pour_mille': '[delinquance]taux_pour_mille'})
  return df_out

def clean_taux_chomage(df: pd.DataFrame) -> pd.DataFrame:
  """
  Supprime les lignes non essentiel
  - change le header à la ligne 4 et supprime les lignes 1 à 3
  - 4 dernieres lignes 

  supprime les colonnes.
  - Libellé

  - Transformer le DataFrame de "wide" en format "long"
  - Extraire l'année de chaque colonne (T1_1982 → 1982)
  - Calculer la moyenne des 4 trimestres pour chaque année et chaque code
  - DataFrame final avec 3 colonnes : Code_departement, annee, [taux_chomage]Taux_moyen

  Parameters
  ----------
  df : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """

  # change le header et supprime les lignes
  df = df.iloc[2:]
  df.columns = df.iloc[0]
  df = df.iloc[1:].reset_index(drop=True)
  df.columns.name = None
  df.drop(df.tail(4).index,inplace = True)

  # supprime les colonnes
  df = df.drop('Libellé', axis=1)

  # reformat et calcul le taux 
  df = df.melt(id_vars=['Code'], var_name='Periode', value_name='Taux')
  df['Code'] = df['Code'].astype(str).str.zfill(2)
  df['Annee'] = df['Periode'].str.split('_').str[1].astype(int)
  df = df.groupby(['Code', 'Annee'])['Taux'].mean().reset_index()
  df.columns = ['Code_departement', 'annee', '[taux_chomage]Taux_moyen']

  return df

def clean_age_moyen(df: pd.DataFrame) -> pd.DataFrame:
  """
  Supprime les colonnes RP_MEASURE, PCS, SEX
  Reformat la colonne GEO et renomme en Code_departement
  renomme la colonne TIME_PERIOD en annee

  Parameters
  ----------
  df : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """

  #Supprime les colonnes
  df = df.drop(['RP_MEASURE', 'PCS', 'SEX'], axis=1)

  # Tri la colone GEO et la rennome
  df['GEO'] = df['GEO'].str.split('-').str[-1]
  df['sort_key'] = df['GEO'].replace({'2A': '1000', '2B': '1001'}).astype(int)
  df = df.sort_values('sort_key').reset_index(drop=True).drop('sort_key', axis=1)
  df['GEO'] = df['GEO'].replace({'1000': '2A', '1001': '2B'})
  df = df.rename(columns={'GEO': 'Code_departement'})

  #renomme la colonne année
  df = df.rename(columns={'TIME_PERIOD': 'annee'})

  #Reformat les colonnes AGE et OBS_VALUE_NIVEAU et renomme les colone reformater et supprime la colone du total
  df = df.pivot(index=['Code_departement', 'annee'], columns='AGE', values='OBS_VALUE_NIVEAU').reset_index()
  df.columns.name = None
  df = df.drop('Y_GE15', axis=1)
  df = df.rename(columns={'Y15T24': '[age_moyen]entre15et24'})
  df = df.rename(columns={'Y25T54': '[age_moyen]entre25et54'})
  df = df.rename(columns={'Y_GE55': '[age_moyen]plus55'})

  return df

def clean_president_sortant(df: pd.DataFrame, metadata_famille_politique: str) -> pd.DataFrame:
  """
  Nettoie les données du président sortant par département.
  - Conserver uniquement les présidentiel T1 et T2
  - Supprimer les colonnes qui ne sont pas utile
  - Reorganiser les colones pour avoir Code_departement, annee, tour, nom, prenom
  - Supprimer les doublon exacte
  - Fusionné les colonnes nom et prénom
  
  Parameters
  ----------
  df : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """

  #conserve uniquement les présidentiels T1 et T2 + annee
  df = df[df['id_election'].str.contains('pres_t1|pres_t2')]
  df[['annee', 'tour']] = df['id_election'].str.extract(r'(\d{4})_pres_(t[12])')
  df = df.drop('id_election', axis=1)
  df['annee'] = df['annee'].astype(int) - 1

  #Supprimer les colonnes inutiles
  df = df.drop(['id_brut_miom', 'code_commune', 'code_bv', 'nuance', 'sexe', 'no_panneau', 'ratio_voix_inscrits', 'ratio_voix_exprimes', 'libelle_abrege_liste', 'nom_tete_liste', 'binome', 'liste', 'libelle_etendu_liste', 'voix'], axis=1)

  #Reorganisation des colonnesCode_departement
  df = df[['code_departement', 'annee', 'tour', 'nom', 'prenom']]

  # supprime les doublon
  df = df.drop_duplicates().reset_index(drop=True)

  # fusionne le nom prénom
  df['candidat'] = df['nom'] + ' ' + df['prenom']
  df = df.drop(['nom', 'prenom'], axis=1)

  # Lire le fichier JSON
  with open(metadata_famille_politique, 'r', encoding='utf-8') as f:
    bords = json.load(f)

  # Créer le mapping et ajouter la colonne
  mapping = {normaliser(item['nom']): item['famille_politique'] for item in bords}
  df['famille_politique'] = df['candidat'].apply(normaliser).map(mapping)

  df = df.rename(columns={'tour': '[president_sortant]tour'})
  df = df.rename(columns={'candidat': '[president_sortant]tour'})
  df = df.rename(columns={'famille_politique': '[president_sortant]tour'})

  df = df.sort_values(['code_departement', 'annee']).reset_index(drop=True)

  return df

def clean_population_active(df: pd.DataFrame, metadata_population_active: str) -> pd.DataFrame:
  """
  Nettoie les données de population active par département.
  - Extrait le code département depuis la colonne GEO (format: 2025-DEP-XX)
  - Garde uniquement TIME_PERIOD (renommé en annee)
  - Supprime les autres colonnes
  - Trie par code département (avec la Corse à la fin)
  
  Parameters
  ----------
  df : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """
  
  # Extraire le code département depuis la colonne GEO (2025-DEP-01 → 01)
  df['Code_departement'] = df['GEO'].str.split('-').str[2]
  df['annee'] = df['TIME_PERIOD'].astype(int) - 1

  # Suppression des colonnes en trop
  df = df.drop(['SEX', 'FREQ', 'RP_MEASURE', 'GEO', 'TIME_PERIOD', 'EDUC'], axis=1)
  
  # Trier par code département avec la Corse (2A, 2B) à la fin
  df['sort_key'] = df['Code_departement'].replace({'2A': '1000', '2B': '1001'})
  df['sort_key'] = pd.to_numeric(df['sort_key'], errors='coerce')
  df = df.sort_values('sort_key').reset_index(drop=True).drop('sort_key', axis=1)

  #Reformat les colonnes AGE et OBS_VALUE_NIVEAU et renomme les colone reformater et supprime la colone du total
  df = df.pivot(index=['Code_departement', 'annee', 'EMPSTA_ENQ'], columns='AGE', values='OBS_VALUE_NIVEAU').reset_index()
  df.columns.name = None
  df = df.drop('Y15T64', axis=1)
  df = df.drop('Y_GE15', axis=1)
  df = df.rename(columns={'Y15T24': '[population_active]entre15et24'})
  df = df.rename(columns={'Y25T54': '[population_active]entre25et54'})
  df = df.rename(columns={'Y55T64': '[population_active]entre55et64'})

  # Lire le fichier JSON
  with open(metadata_population_active, 'r', encoding='utf-8') as f:
    bords = json.load(f)

  # Créer le mapping et ajouter la colonne
  mapping = {normaliser(item['EMPSTA_ENQ']): item['Statut_emploi'] for item in bords}
  df['Statut_emploi'] = df['EMPSTA_ENQ'].apply(normaliser).map(mapping)

  #Regorganisation des colonnes et supprime l'emploie chiffre
  df = df.drop('EMPSTA_ENQ', axis=1)
  df = df[['Code_departement', 'annee', 'Statut_emploi', '[population_active]entre15et24', '[population_active]entre25et54', '[population_active]entre55et64']]
  df = df.fillna(0)

  return df

def clean_categorie_professionnelle(df: pd.DataFrame, metadata_categorie_professionnelle: str) -> pd.DataFrame:
  """
  Nettoie les données des categorie professionne par département.
  - Supprime les colonnes non utilisé
  - tri par date croissante
  - supprime les lignes qui contienne le cumule des categories
  - map les metadata avec le bon code categorie
  - renomme les colonnes
  
  Parameters
  ----------
  df : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """
  
  # Suppression des colonnes en trop
  df = df.drop(['IMMI', 'EEC_MEASURE', 'SEX', 'EDUC', 'UNDEREMP', 'EMPFORM', 'UNEMPDUR', 'COMPOHALO', 'EMPSTA', 'WKTIME', 'ACTIVITY', 'AGE', 'OBS_STATUS', 'UNIT_MULT', 'UNIT_MEASURE'], axis=1)

  # tri les lignes par ordre croissant
  df['TIME_PERIOD'] = pd.to_numeric(df['TIME_PERIOD'], errors='coerce')
  df = df.sort_values('TIME_PERIOD').reset_index(drop=True)

  #supprimer les lignes où la colonne PCS contient la valeur _T
  df = df[~df["PCS"].str.contains("_T", na=False)]

  # Créer le mapping et ajouter la colonne
  with open(metadata_categorie_professionnelle, 'r', encoding='utf-8') as f:
    bords = json.load(f)

  mapping = {normaliser(item['code']): item['libelle'] for item in bords}
  df['PCS'] = df['PCS'].apply(normaliser).map(mapping)

  # pivote la table
  df = df.pivot_table(
    index="TIME_PERIOD", 
    columns="PCS",
    values="OBS_VALUE_NIVEAU",
    aggfunc="sum"
  ).reset_index()

  #renome les colone
  df = df.rename(columns={"TIME_PERIOD": "annee"})
  df.columns = [
    col if col == "annee" else f"[categorie_professionnelle] {col}"
    for col in df.columns
  ]
  df.columns.name = None

  return df

def clean_equipement_sportif(df: pd.DataFrame) -> pd.DataFrame:
  """
  Nettoie les données des categorie professionne par département.
  - Supprime les colonnes non utilisé
  - Determination de la date d'installation
  - Determination de la date de suppression
  - Suppression des colonne de base
  
  Parameters
  ----------
  df : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """
  
  # conservation des colonnes importantes
  df = df.loc[:, [
    "dep_code",
    "equip_service_date",
    "equip_service_periode",
    "inst_hs_bool",
    "inst_date_etat"
  ]]

  # Determination de la date d'installation
  df["equip_service_date"] = pd.to_datetime(df["equip_service_date"], errors="coerce")
  annee_date = df["equip_service_date"].dt.year
  annees = df["equip_service_periode"].str.extract(r"(\d{4})(?:-(\d{4}))?").astype(float)
  annee_periode = annees[1].fillna(annees[0])
  df["equip_service_annee"] = annee_date.fillna(annee_periode).astype("Int64")

  # Determination de la date de suppression
  annee_actuelle = pd.Timestamp.now().year
  df["inst_date_etat"] = pd.to_datetime(df["inst_date_etat"], errors="coerce")
  annee_etat = df["inst_date_etat"].dt.year
  df["dern_inst_date"] = np.where(
      df["inst_hs_bool"] == True,
      annee_etat,
      annee_actuelle
  )
  df["dern_inst_date"] = df["dern_inst_date"].astype("Int64")

  #Suppression des colonne de base
  df = df.drop(columns=['equip_service_date', 'equip_service_periode', 'inst_hs_bool', 'inst_date_etat'])

  #Comptage des equipements par année par departement
  df_temp = df[["dep_code", "equip_service_annee", "dern_inst_date"]].copy()
  # +1 l'année de mise en service
  debut = df_temp.groupby(
      ["dep_code", "equip_service_annee"]
  ).size().rename("delta").reset_index()
  debut = debut.rename(columns={"equip_service_annee": "annee"})
  debut["delta"] = debut["delta"]
  # -1 l'année suivant la fin
  fin = df_temp.groupby(
      ["dep_code", "dern_inst_date"]
  ).size().rename("delta").reset_index()
  fin = fin.rename(columns={"dern_inst_date": "annee"})
  fin["annee"] = fin["annee"] + 1
  fin["delta"] = -fin["delta"]
  # Combiner
  variations = pd.concat([debut, fin], ignore_index=True)
  annee_min = df["equip_service_annee"].min()
  annee_max = df["dern_inst_date"].max()
  deps = df["dep_code"].unique()
  annees = range(annee_min, annee_max + 1)
  index = pd.MultiIndex.from_product([deps, annees], names=["dep_code", "annee"])
  base = pd.DataFrame(index=index).reset_index()
  # Ajouter les deltas
  base = base.merge(variations, on=["dep_code", "annee"], how="left")
  base["delta"] = base["delta"].fillna(0)
  # Cumul par département = nombre d'équipements en service
  base["nb_equipements"] = base.groupby("dep_code")["delta"].cumsum()
  base = base[base["annee"] >= 1950]
  deps_actifs = (
    base.groupby("dep_code")["nb_equipements"]
    .max()
    .loc[lambda x: x > 0]
    .index
  )
  base = base[
    base["dep_code"].isin(deps_actifs) &
    (base["nb_equipements"] > 0)
  ]
  base = base.sort_values(by=["annee", "dep_code"]).reset_index(drop=True)

  #renomme les colonnes
  base = base.drop(columns=['delta'])
  base = base.rename(columns={'dep_code': 'Code_departement'})
  base = base.rename(columns={'nb_equipements': '[equipement_sportif]nb_equipements'})

  return base

def clean_revenu_moyen(dfs: dict) -> pd.DataFrame:
  """
  Nettoie les données du revenu moyen par département.
  - 8420 
    - Supprime la feuille 'notice'
      - Supprime les lignes vides
      - Supprime les colone non importante
      - calcule le revenue moyen par foyer
      - Renomme les colones
      - reformat les departement

  Parameters
  ----------
  dfs : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """
  
  # Supprime la feuille 'notice' uniquement pour l'année '8420'
  dfs["8420"] = {
    sheet_name: df
    for sheet_name, df in dfs["8420"].items()
    if sheet_name.lower() != "notice"
  }

  # Supprime les premiere ligne vide et reajuste l'index
  df = dfs["8420"]['1984_1999']
  df = df.iloc[7:]
  df.columns = df.iloc[0]
  df = df.iloc[1:].reset_index(drop=True)
  df.columns.name = None

  # Supprime la premiere colone vide et les colonnes non utile
  df = df.iloc[:, 1:]
  df = df.iloc[:, :-5]
  df = df.drop(columns=['Nom'])

  # calcule les revenue moyen par foyer
  df["Revenu net imposable moyen"] = (
    df["Revenus nets imposables"] / df["Nombre de foyers fiscaux"]
  )
  df = df.drop(columns=['Revenus nets imposables', 'Nombre de foyers fiscaux'])

  # renomme les colonnes
  df.columns = [
    "Code_departement",
    "annee",
    "[revenu_moyen]revenu_moyen_par_foyer"
  ] 

  #traitement de code departement
  def fix_departement(code):
    """
    Convertit les codes département bruts vers le format standard français.
    
    Exemples :
      '010' → '01'
      '100' → '10'
      '2A0' → '2A'
      '2B0' → '2B'
      '971' → '971' (DOM-TOM, inchangé)
    """
    # Corse
    if code in ('2A0', '2B0'):
        return code[:-1]  # '2A0' → '2A'
    
    # DOM-TOM (971, 972, 973, 974, 976...)
    if int(code) >= 970:
        return code  # inchangé
    
    # Métropole
    return str(int(code) // 10).zfill(2)
  df['Code_departement'] = df['Code_departement'].apply(fix_departement)
  dfs["8420"]['1984_1999'] = df

  # Supprime les premiere ligne vide et reajuste l'index
  df = dfs["8420"]['2000_2017']
  df = df.iloc[7:]
  df.columns = df.iloc[0]
  df = df.iloc[1:].reset_index(drop=True)
  df.columns.name = None

  # Supprime la premiere colone vide et les colonnes non utile
  df = df.iloc[:, 1:]
  df = df.iloc[:, :-7]
  df = df.drop(columns=['Nom'])

  # calcule les revenue moyen par foyer
  df["Revenu net imposable moyen"] = (
    df["Revenu fiscal de référence"] / df["Nombre de foyers fiscaux"]
  )
  df = df.drop(columns=['Revenu fiscal de référence', 'Nombre de foyers fiscaux'])

  # renomme les colonnes
  df.columns = [
    "Code_departement",
    "annee",
    "[revenu_moyen]revenu_moyen_par_foyer"
  ] 

  #traitement de code departement
  df = df[df["Code_departement"] != "B31"]
  df['Code_departement'] = df['Code_departement'].apply(fix_departement)

  dfs["8420"]['2000_2017'] = df

  # Supprime les premiere ligne vide et reajuste l'index
  df = dfs["8420"]['2018']
  df = df.iloc[7:]
  df.columns = df.iloc[0]
  df = df.iloc[1:].reset_index(drop=True)
  df.drop(df.tail(4).index, inplace=True)
  df.columns.name = None

  # Supprime la premiere colone vide et les colonnes non utile
  df = df.iloc[:, 1:]
  df = df.iloc[:, :-9]
  df = df.drop(columns=['Nom'])

  # calcule les revenue moyen par foyer
  df["Revenu net imposable moyen"] = (
    df["Revenu fiscal de référence"] / df["Nombre de foyers fiscaux"]
  )
  df = df.drop(columns=['Revenu fiscal de référence', 'Nombre de foyers fiscaux'])

  # renomme les colonnes
  df.columns = [
    "Code_departement",
    "annee",
    "[revenu_moyen]revenu_moyen_par_foyer"
  ] 

  #traitement de code departement
  df = df[df["Code_departement"] != "B31"]
  df['Code_departement'] = df['Code_departement'].apply(fix_departement)

  dfs["8420"]['2018'] = df

  # Supprime les premiere ligne vide et reajuste l'index
  df = dfs["8420"]['2019_2020']
  df = df.iloc[7:]
  df.columns = df.iloc[0]
  df = df.iloc[1:].reset_index(drop=True)
  df.columns.name = None

  # Supprime la premiere colone vide et les colonnes non utile
  df = df.iloc[:, 1:]
  df = df.iloc[:, :-7]
  df = df.drop(columns=['Nom'])

  # calcule les revenue moyen par foyer
  df["Revenu net imposable moyen"] = (
    df["Revenu fiscal de référence"] / df["Nombre de foyers fiscaux"]
  )
  df = df.drop(columns=['Revenu fiscal de référence', 'Nombre de foyers fiscaux'])

  # renomme les colonnes
  df.columns = [
    "Code_departement",
    "annee",
    "[revenu_moyen]revenu_moyen_par_foyer"
  ] 

  #traitement de code departement
  df = df[df["Code_departement"] != "B31"]
  df['Code_departement'] = df['Code_departement'].apply(fix_departement)

  dfs["8420"]['2019_2020'] = df

  dfs["8420"] = pd.concat(dfs["8420"].values(), ignore_index=True)

  # Supprime les premiere ligne vide et reajuste l'index
  df = dfs['21']['Feuil1']
  df = df.iloc[6:]
  df.columns = df.iloc[0]
  df = df.iloc[1:].reset_index(drop=True)
  df.columns.name = None

  # Supprime la premiere colone vide et les colonnes non utile
  df = df.iloc[:, 1:]
  df = df.iloc[:, :-7]
  df = df[df.iloc[:, 3].astype(str).str.strip().str.lower() == "total"]

  #traitement de code departement
  df = df[df.iloc[:, 0] != "B31"]

  # renomme les colonnes
  df.columns = [
    "Code_departement",
    "commune",
    "libelle_commune",
    "tranche",
    "nbr_foyer",
    "revenue_referance"
  ] 

  # calcule les revenue moyen par foyer
  for col in ["revenue_referance", "nbr_foyer"]:
    df[col] = (
      df[col]
      .astype(str)
      .str.replace(" ", "")
      .str.replace(",", ".")
    )
    df[col] = pd.to_numeric(df[col], errors="coerce")

  df["[revenu_moyen]revenu_moyen_par_foyer"] = (
    df["revenue_referance"] * 1000 / df["nbr_foyer"]
  )
  df = df.drop(columns=['revenue_referance', "nbr_foyer", 'libelle_commune', 'commune', 'tranche'])

  df['Code_departement'] = df['Code_departement'].apply(fix_departement)
  df = df.reset_index(drop=True)

  df = (
    df.groupby("Code_departement", as_index=False)["[revenu_moyen]revenu_moyen_par_foyer"]
    .mean()
  )
  df["annee"] = 2021
  df = df[
    [
        "Code_departement",
        "annee",
        "[revenu_moyen]revenu_moyen_par_foyer"
    ]
  ]
  dfs["21"] = df

  # Supprime les premiere ligne vide et reajuste l'index
  df = dfs["22"]['Feuil1']
  df = df.iloc[4:]
  df.columns = df.iloc[0]
  df = df.iloc[2:].reset_index(drop=True)
  df.drop(df.tail(2).index, inplace=True)
  df.columns.name = None

  # Supprime la premiere colone vide et les colonnes non utile
  df = df.iloc[:, :-7]
  df = df[df.iloc[:, 3].astype(str).str.strip().str.lower() == "total"]

  # calcule les revenue moyen par foyer
  for col in ["Revenu fiscal de référence des foyers fiscaux", "Nombre de foyers fiscaux"]:
    df[col] = (
      df[col]
      .astype(str)
      .str.replace(" ", "")
      .str.replace(",", ".")
    )
    df[col] = pd.to_numeric(df[col], errors="coerce")

  df["[revenu_moyen]revenu_moyen_par_foyer"] = (
    df["Revenu fiscal de référence des foyers fiscaux"] * 1000 / df["Nombre de foyers fiscaux"]
  )
  df = df.drop(columns=['Revenu fiscal de référence des foyers fiscaux', "Nombre de foyers fiscaux", 'Libellé de la commune', 'Commune', 'Revenu fiscal de référence par tranche (en euros)'])

  #traitement de code departement
  df = df[df.iloc[:, 0] != "B31"]
  df['Code_departement'] = df['Dép.'].apply(fix_departement)
  df = df.drop(columns=['Dép.'])
  df = df.reset_index(drop=True)

  df["annee"] = 2022
  df = df[
    [
        "Code_departement",
        "annee",
        "[revenu_moyen]revenu_moyen_par_foyer"
    ]
  ]
  dfs["22"] = df

  # Supprime les premiere ligne vide et reajuste l'index
  df = dfs["23"]['ListeCommune']
  df = df.iloc[4:]
  df.columns = df.iloc[0]
  df = df.iloc[2:].reset_index(drop=True)
  # df.drop(df.tail(2).index, inplace=True)
  df.columns.name = None

  # Supprime la premiere colone vide et les colonnes non utile
  df = df.iloc[:, :-7]
  df = df[df.iloc[:, 3].astype(str).str.strip().str.lower() == "total"]

  # calcule les revenue moyen par foyer
  for col in ["Revenu fiscal de référence des foyers fiscaux", "Nombre de foyers fiscaux"]:
    df[col] = (
      df[col]
      .astype(str)
      .str.replace(" ", "")
      .str.replace(",", ".")
    )
    df[col] = pd.to_numeric(df[col], errors="coerce")

  df["[revenu_moyen]revenu_moyen_par_foyer"] = (
    df["Revenu fiscal de référence des foyers fiscaux"] * 1000 / df["Nombre de foyers fiscaux"]
  )
  df = df.drop(columns=['Revenu fiscal de référence des foyers fiscaux', "Nombre de foyers fiscaux", 'Libellé de la commune', 'Commune', 'Revenu fiscal de référence par tranche (en euros)'])

  #traitement de code departement
  df = df[df.iloc[:, 0] != "B31"]
  df['Code_departement'] = df['Dép.'].apply(fix_departement)
  df = df.drop(columns=['Dép.'])
  df = df.reset_index(drop=True)

  df["annee"] = 2023
  df = df[
    [
        "Code_departement",
        "annee",
        "[revenu_moyen]revenu_moyen_par_foyer"
    ]
  ]
  dfs["23"] = df

  df_final = pd.concat(
    [dfs["8420"], dfs["21"], dfs["22"], dfs["23"]],
    ignore_index=True
  )

  return df_final

def clean_etablissement_culturel(df: pd.DataFrame) -> pd.DataFrame:
  """
  Nettoie les données du revenu moyen par département.
  - supprime les colonnes non utile
  - Renomme les colonnes

  Parameters
  ----------
  df : pd.DataFrame
  
  Returns
  -------
  pd.DataFrame
  """

  # Suppression des colones inutile
  df = df.drop(columns=['pct_culturel', 'nombre_etablissements', 'libelle_geographique'])

  # Renomme les colonnes
  df.columns = [
    "annee",
    "Code_departement",
    "[etablissement_culturel]nombre_etablissements"
  ]

  df = df[
    [
        "Code_departement",
        "annee",
        "[etablissement_culturel]nombre_etablissements"
    ]
  ]

  return df