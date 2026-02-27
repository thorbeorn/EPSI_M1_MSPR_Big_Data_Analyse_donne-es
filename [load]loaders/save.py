from sqlalchemy import create_engine
import pandas as pd
import logging

# Configuration du logging
logger = logging.getLogger(__name__)

# Création du moteur de connexion à la base MySQL
engine = create_engine(
    "mysql+pymysql://mspr-user:z9k5RYgeDr3457TV33tY2eLPgd36XE5y88LAcCpz@localhost:3306/mspr-db"
)

def build_indicateurs_table(dfs: dict):
    """
    Construit la table 'indicateurs' contenant toutes les combinaisons
    uniques (Code_departement, annee) trouvées dans les DataFrames Silver.
    """

    logger.info("Construction de la table indicateurs")

    cache = []

    for var_name, df in dfs.items():
        try:
            if var_name.startswith("silver_") and isinstance(df, pd.DataFrame):

                cols = df.columns

                # Cas 1 : les deux colonnes existent
                if "Code_departement" in cols and "annee" in cols:
                    subset = df[["Code_departement", "annee"]].copy()
                    cache.append(subset)

                # Cas 2 : seulement Code_departement
                elif "Code_departement" in cols:
                    subset = df[["Code_departement"]].copy()
                    subset["annee"] = None
                    cache.append(subset)

                # Cas 3 : seulement annee
                elif "annee" in cols:
                    subset = df[["annee"]].copy()
                    subset["Code_departement"] = None
                    cache.append(subset)

                else:
                    logger.debug(f"Aucune clé trouvée dans {var_name}")

        except Exception as e:
            logger.error(f"Erreur traitement indicateurs pour {var_name}: {e}", exc_info=True)

    if not cache:
        logger.warning("Aucune donnée pour construire la table indicateurs")
        return

    # Fusion de toutes les clés
    indicateurs_df = pd.concat(cache, ignore_index=True)

    # Nettoyage
    indicateurs_df = indicateurs_df.drop_duplicates()
    indicateurs_df = indicateurs_df.dropna(subset=["Code_departement", "annee"], how="all")

    # Types
    indicateurs_df["Code_departement"] = indicateurs_df["Code_departement"].astype(str)
    indicateurs_df["annee"] = pd.to_numeric(indicateurs_df["annee"], errors="coerce")

    indicateurs_df = indicateurs_df.drop_duplicates()

    logger.info(f"{len(indicateurs_df)} lignes dans la table indicateurs")

    # Sauvegarde MySQL
    indicateurs_df.to_sql(
        name="indicateurs",
        con=engine,
        if_exists="replace",
        index=False
    )

    logger.info("Table indicateurs sauvegardée avec succès")

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
            
    build_indicateurs_table(dfs)
    logger.info("Fin de la sauvegarde des DataFrames Silver")