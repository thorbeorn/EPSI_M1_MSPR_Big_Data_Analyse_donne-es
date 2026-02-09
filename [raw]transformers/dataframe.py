import pandas as pd

def agregation_delinquance_departement(df: pd.DataFrame) -> pd.DataFrame:
  """
  Agrège les données par département et année.
  
  - Somme de 'nombre'
  - Moyenne de 'taux_pour_mille'
  - Supprime les autres colonnes
  
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
