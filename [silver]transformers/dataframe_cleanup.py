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
  df['Annee'] = df['Periode'].str.split('_').str[1].astype(int)
  df = df.groupby(['Code', 'Annee'])['Taux'].mean().reset_index()
  df.columns = ['Code_departement', 'annee', '[taux_chomage]Taux_moyen']

  return df