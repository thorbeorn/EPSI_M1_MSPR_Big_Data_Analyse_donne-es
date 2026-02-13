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

def clean_revenu_moyen(df: pd.DataFrame) -> pd.DataFrame:
  """
  Nettoie les données du revenu moyen par département.
  - Extrait le code département depuis la colonne GEO (format: 2025-DEP-XX)
  - Garde uniquement TIME_PERIOD (renommé en annee) et OBS_VALUE_NIVEAU (renommé en [revenu]moyen)
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
  
  # Sélectionner et renommer les colonnes
  df = df[['Code_departement', 'TIME_PERIOD', 'OBS_VALUE_NIVEAU']].copy()
  df.columns = ['Code_departement', 'annee', '[revenu]moyen']
  
  # Convertir les types
  df['annee'] = df['annee'].astype(int)
  df['[revenu]moyen'] = pd.to_numeric(df['[revenu]moyen'], errors='coerce')
  
  # Trier par code département avec la Corse (2A, 2B) à la fin
  df['sort_key'] = df['Code_departement'].replace({'2A': '1000', '2B': '1001'})
  df['sort_key'] = pd.to_numeric(df['sort_key'], errors='coerce')
  df = df.sort_values('sort_key').reset_index(drop=True).drop('sort_key', axis=1)
  
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

  df['candidat'].to_csv('candidats.csv', index=False, header=False)

  df = df.rename(columns={'tour': '[president_sortant]tour'})
  df = df.rename(columns={'candidat': '[president_sortant]tour'})
  df = df.rename(columns={'famille_politique': '[president_sortant]tour'})

  df = df.sort_values(['code_departement', 'annee']).reset_index(drop=True)

  return df