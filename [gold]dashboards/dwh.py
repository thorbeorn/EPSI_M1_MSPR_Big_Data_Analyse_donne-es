import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import logging
import time

# CONFIGURATION DU LOGGING
logger = logging.getLogger(__name__)
# CONNEXION BASE DE DONNÉES
engine = create_engine(
    "mysql+pymysql://mspr-user:z9k5RYgeDr3457TV33tY2eLPgd36XE5y88LAcCpz@localhost:3306/mspr-db"
)

# FONCTION GÉNÉRIQUE : UPLOAD DATAFRAME → MINIO
def upload_df_to_minio(
    df: pd.DataFrame,
    file_format: str,  # "csv" ou "parquet"
    bucket_name="data-lake",
    object_name=None,
    endpoint="localhost:9000",
    access_key="mspr-admin",
    secret_key="4A724rhUh65XMHvVR9k73xumLhytHtm557VKC83G"
):
    """
    Upload un DataFrame Pandas en CSV ou Parquet directement dans MinIO
    sans création de fichier local.
    """

    logger.debug(f"Début upload vers bucket '{bucket_name}' au format {file_format}")

    if file_format not in ["csv", "parquet"]:
        raise ValueError("file_format doit être 'csv' ou 'parquet'")

    # Génération d’un nom horodaté si non fourni
    if object_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"export_{timestamp}.{file_format}"

    try:
        start_time = time.time()

        buffer = BytesIO()

        # Conversion du DataFrame
        logger.debug("Conversion du DataFrame en mémoire")

        if file_format == "csv":
            df.to_csv(buffer, index=False)
            content_type = "text/csv"

        elif file_format == "parquet":
            df.to_parquet(buffer, index=False, engine="pyarrow")
            content_type = "application/octet-stream"

        buffer.seek(0)

        # Initialisation client MinIO
        logger.debug("Connexion au serveur MinIO")

        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        # Création du bucket si inexistant
        if not client.bucket_exists(bucket_name):
            logger.warning(f"Bucket '{bucket_name}' inexistant → création")
            client.make_bucket(bucket_name)

        # Upload objet
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type=content_type
        )

        duration = round(time.time() - start_time, 2)

        logger.debug(
            f"Upload réussi : {bucket_name}/{object_name} "
            f"| lignes={len(df)} "
            f"| taille={buffer.getbuffer().nbytes} bytes "
            f"| durée={duration}s"
        )

    except S3Error as err:
        logger.error(f"Erreur MinIO : {err}")
        raise

    except Exception as e:
        logger.exception(f"Erreur inattendue lors de l'upload : {e}")
        raise


# GOLD LAYER : INDICATEURS COMPLETS
def create_gold_all_indicator_df():
    """
    Construit la table GOLD consolidée des indicateurs
    (jointure multi-tables département + année)
    puis exporte en CSV et Parquet vers MinIO.
    """

    logger.info("Création dataset GOLD - all_indicator")

    query = """ 
    SELECT 
        base.Code_departement,
        base.annee,

        -- Age
        am.`[age_moyen]entre15et24`,
        am.`[age_moyen]entre25et54`,
        am.`[age_moyen]plus55`,
         
        -- Délinquance
        d.`[delinquance]nombre`,
        d.`[delinquance]taux_pour_mille`,

        -- Revenu
        rm.`[revenu_moyen]revenu_moyen_par_foyer`,

        -- Chômage
        tc.`[taux_chomage]Taux_moyen`,

        -- Equipements
        es.`[equipement_sportif]nb_equipements`,

        -- Culture
        ec.`[etablissement_culturel]nombre_etablissements`,

        -- Niveau d'étude
        ne.`[niveau_etude]Aucun diplôme`,
        ne.`[niveau_etude]BEPC, brevet élémentaire, brevet des collèges, DNB`,
        ne.`[niveau_etude]Baccalauréat universitaire ou équivalent`,
        ne.`[niveau_etude]Baccalauréat, brevet professionnel ou équivalent`,
        ne.`[niveau_etude]CAP, BEP ou diplôme de niveau équivalent`,
        ne.`[niveau_etude]CEP (certificat d’études primaires)`,
        ne.`[niveau_etude]Diplôme d'études supérieures`,
        ne.`[niveau_etude]Diplôme de niveau bac + 5 ou plus`,
        ne.`[niveau_etude]Diplôme universitaire 2e ou 3e cycle`,
        ne.`[niveau_etude]Enseignement supérieur de cycle court`,

        -- Population active cumulée
        pa.`[population_active]pop_15_24`,
        pa.`[population_active]pop_25_54`,
        pa.`[population_active]pop_55_64`,

        -- categorie_professionnelle
        cp.`[categorie_professionnelle] Agriculteurs`,
        cp.`[categorie_professionnelle] Artisans, commerçants et patron`,
        cp.`[categorie_professionnelle] Autres`,
        cp.`[categorie_professionnelle] Cadres et professions supérieures`,
        cp.`[categorie_professionnelle] Employés`,
        cp.`[categorie_professionnelle] Employés peu qualifiés`,
        cp.`[categorie_professionnelle] Employés qualifiés`,
        cp.`[categorie_professionnelle] Ouvriers peu qualifiés`,
        cp.`[categorie_professionnelle] Ouvriers qualifiés`,
        cp.`[categorie_professionnelle] Professions intermédiaires`,

        -- pouvoir_achat
        pa2.`[pouvoir_achat]pourcentage_annee_precedente` AS `[pouvoir_achat]pourcentage_annee_precedente`,
        pa2.`Pouvoir d’achat du revenu disponible brut` AS `[pouvoir_achat]Pouvoir d’achat du revenu disponible brut`,

        -- Abstention et votant
        av.`inscrits_total` AS `[abstention_votant]inscrits`,
        av.`abstentions_total` AS `[abstention_votant]abstentions`,
        av.`blancs_total` AS `[abstention_votant]blancs`

    FROM indicateurs base

    LEFT JOIN age_moyen am 
        ON am.Code_departement = base.Code_departement 
        AND am.annee = base.annee
    
    LEFT JOIN delinquance d 
        ON d.Code_departement = base.Code_departement 
        AND d.annee = base.annee

    LEFT JOIN revenu_moyen rm 
        ON rm.Code_departement = base.Code_departement 
        AND rm.annee = base.annee

    LEFT JOIN taux_chommage tc 
        ON tc.Code_departement = base.Code_departement 
        AND tc.annee = base.annee

    LEFT JOIN equipement_sportif es 
        ON es.Code_departement = base.Code_departement 
        AND es.annee = base.annee

    LEFT JOIN etablissement_culturel ec 
        ON ec.Code_departement = base.Code_departement 
        AND ec.annee = base.annee

    LEFT JOIN niveau_etude ne 
        ON ne.Code_departement = base.Code_departement 
        AND ne.annee = base.annee

    LEFT JOIN (
        SELECT
            Code_departement,
            annee,
            SUM(`[population_active]entre15et24`) AS `[population_active]pop_15_24`,
            SUM(`[population_active]entre25et54`) AS `[population_active]pop_25_54`,
            SUM(`[population_active]entre55et64`) AS `[population_active]pop_55_64`
        FROM population_active
        GROUP BY Code_departement, annee
    ) pa
    ON pa.Code_departement = base.Code_departement
    AND pa.annee = base.annee

    LEFT JOIN categorie_professionnelle cp 
        ON cp.annee = base.annee

    LEFT JOIN pouvoir_achat pa2
        ON pa2.annee = base.annee

    LEFT JOIN (
        SELECT
            code_departement,
            annee,
            SUM(`[abstention_votant]inscrits`) AS inscrits_total,
            SUM(`[abstention_votant]abstentions`) AS abstentions_total,
            SUM(`[abstention_votant]blancs`) AS blancs_total
        FROM abstention_votant
        WHERE `[abstention_votant]tour` = "t2"
        GROUP BY code_departement, annee
    ) av
    ON av.code_departement = base.Code_departement
    AND av.annee = base.annee
   
  WHERE
    am.`[age_moyen]entre15et24` IS NOT NULL
    AND am.`[age_moyen]entre25et54` IS NOT NULL
    AND am.`[age_moyen]plus55` IS NOT NULL
    AND d.`[delinquance]nombre` IS NOT NULL
    AND d.`[delinquance]taux_pour_mille` IS NOT NULL
    AND rm.`[revenu_moyen]revenu_moyen_par_foyer` IS NOT NULL
    AND tc.`[taux_chomage]Taux_moyen` IS NOT NULL
    AND es.`[equipement_sportif]nb_equipements` IS NOT NULL
    AND ec.`[etablissement_culturel]nombre_etablissements` IS NOT NULL
    AND pa.`[population_active]pop_15_24` IS NOT NULL
    AND pa.`[population_active]pop_25_54` IS NOT NULL
    AND pa.`[population_active]pop_55_64` IS NOT NULL
    AND av.inscrits_total IS NOT NULL
    AND av.abstentions_total IS NOT NULL
    AND av.blancs_total IS NOT NULL;
    """

    try:
        start_time = time.time()

        logger.debug("Exécution requête SQL indicateurs")
        df = pd.read_sql(query, engine)

        logger.debug(f"Requête exécutée | lignes récupérées : {len(df)}")

        # Upload CSV
        upload_df_to_minio(
            df,
            file_format="csv",
            bucket_name="gold",
            object_name="all_indicator.csv"
        )

        # Upload Parquet
        upload_df_to_minio(
            df,
            file_format="parquet",
            bucket_name="gold",
            object_name="all_indicator.parquet"
        )

        duration = round(time.time() - start_time, 2)
        logger.info(f"Dataset all_indicator terminé en {duration}s")

    except Exception as e:
        logger.exception(f"Erreur lors de la création du GOLD indicateur : {e}")
        raise

# GOLD LAYER : PRESIDENT SORTANT (VAINQUEUR T2)
def create_gold_all_president_df():
    """
    Construit la table GOLD des présidents gagnants au second tour
    (max nombre de voix par département et année).
    """

    logger.info("Création dataset GOLD - all_president")

    query = """ 
    SELECT 
        p.annee,
        p.code_departement,
        p.`[president_sortant]tour` AS `[president_sortant]tour`,
        p.`[president_sortant]famille_politique` AS `[president_sortant]famille_politique`,
        ROUND(
            100 * SUM(p.`[president_sortant]nombre_voix`) / t.total_voix,
            2
        ) AS `[president_sortant]pourcentage`
    FROM president_sortant p
    JOIN (
        SELECT 
            annee,
            code_departement,
            `[president_sortant]tour`,
            SUM(`[president_sortant]nombre_voix`) AS total_voix
        FROM president_sortant
        GROUP BY annee, code_departement, `[president_sortant]tour`
    ) t
    ON p.annee = t.annee 
    AND p.code_departement = t.code_departement
    AND p.`[president_sortant]tour` = t.`[president_sortant]tour`
    GROUP BY 
        p.annee,
        p.code_departement,
        p.`[president_sortant]tour`,
        p.`[president_sortant]famille_politique`,
        t.total_voix
    ORDER BY 
        p.annee,
        p.code_departement,
        `[president_sortant]tour`,
        `[president_sortant]famille_politique`;
    """

    try:
        start_time = time.time()

        logger.debug("Exécution requête SQL président")
        df = pd.read_sql(query, engine)

        logger.debug(f"Requête exécutée | lignes récupérées : {len(df)}")

        # Upload CSV
        upload_df_to_minio(
            df,
            file_format="csv",
            bucket_name="gold",
            object_name="all_president.csv"
        )

        # Upload Parquet
        upload_df_to_minio(
            df,
            file_format="parquet",
            bucket_name="gold",
            object_name="all_president.parquet"
        )

        duration = round(time.time() - start_time, 2)
        logger.info(f"Dataset all_president terminé en {duration}s")

    except Exception as e:
        logger.exception(f"Erreur lors de la création du GOLD president : {e}")
        raise