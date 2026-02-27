from sqlalchemy import create_engine
import pandas as pd
import logging

# Configuration du logging
logger = logging.getLogger(__name__)

# Création du moteur de connexion à la base MySQL
engine = create_engine(
    "mysql+pymysql://mspr-user:z9k5RYgeDr3457TV33tY2eLPgd36XE5y88LAcCpz@localhost:3306/mspr-db"
)

# Fonction : sauvegarde automatique des DataFrames Silver
def save_all_silver_dataframes(dfs: dict):
    """
    Parcourt un dictionnaire contenant des objets Python.
    Sauvegarde dans MySQL tous les objets :
      - dont le nom commence par 'silver_'
      - qui sont des pandas DataFrame

    Le nom de la table est dérivé automatiquement :
        silver_clients_df  ->  clients
    """

    logger.info("Début de la sauvegarde des DataFrames Silver")

    for var_name, var_value in dfs.items():
        try:
            # Vérifie que la variable correspond à un DataFrame Silver
            if var_name.startswith("silver_") and isinstance(var_value, pd.DataFrame):

                # Construction du nom de table :
                # suppression du préfixe 'silver_' et du suffixe '_df'
                table_name = (
                    str(var_name)
                    .removeprefix("silver_")
                    .removesuffix("_df")
                )

                logger.debug(f"Sauvegarde en cours : {var_name} -> table '{table_name}'")

                # Sauvegarde dans MySQL
                var_value.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists="replace",  # Remplace la table si elle existe
                    index=False
                )

                logger.debug(f"Sauvegarde terminée pour : {table_name}")

            else:
                # Cas où la variable ne correspond pas au format attendu
                logger.warning(f"Ignoré : {var_name} (non Silver ou non DataFrame)")

        except Exception as e:
            # Gestion des erreurs pour éviter l'arrêt complet du processus
            logger.error(
                f"Erreur lors de la sauvegarde de {var_name} : {str(e)}",
                exc_info=True
            )

    logger.info("Fin de la sauvegarde des DataFrames Silver")