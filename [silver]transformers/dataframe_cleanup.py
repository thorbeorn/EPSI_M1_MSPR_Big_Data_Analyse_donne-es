import pandas as pd

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
  df = df.drop('RP_MEASURE', axis=1)
  df = df.drop('PCS', axis=1)
  df = df.drop('SEX', axis=1)

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

