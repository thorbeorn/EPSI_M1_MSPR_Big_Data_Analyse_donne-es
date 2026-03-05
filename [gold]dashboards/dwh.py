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

    -- Age (toutes les tranches)
    am.`[age_moyen]0 à 4 ans`,
    am.`[age_moyen]5 à 9 ans`,
    am.`[age_moyen]10 à 14 ans`,
    am.`[age_moyen]15 à 19 ans`,
    am.`[age_moyen]20 à 24 ans`,
    am.`[age_moyen]25 à 29 ans`,
    am.`[age_moyen]30 à 34 ans`,
    am.`[age_moyen]35 à 39 ans`,
    am.`[age_moyen]40 à 44 ans`,
    am.`[age_moyen]45 à 49 ans`,
    am.`[age_moyen]50 à 54 ans`,
    am.`[age_moyen]55 à 59 ans`,
    am.`[age_moyen]60 à 64 ans`,
    am.`[age_moyen]65 à 69 ans`,
    am.`[age_moyen]70 à 74 ans`,
    am.`[age_moyen]75 à 79 ans`,
    am.`[age_moyen]80 ans et plus`,

    -- Délinquance
    d.`[delinquance]nombre`,
    d.`[delinquance]taux_pour_mille`,

    -- Chômage
    tc.`[taux_chomage]Taux_moyen`,

    -- Equipements
    es.`[equipement_sportif]nb_equipements`,

    -- Culture
    ec.`[etablissement_culturel]nombre_etablissements`,

    -- Niveau d'étude
    ne.`[niveau_etude]Aucun diplôme, CEP`,
    ne.`[niveau_etude]Brevet des collèges`,
    ne.`[niveau_etude]CAP, BEP ou équivalent`,
    ne.`[niveau_etude]Baccalauréat ou équivalent`,
    ne.`[niveau_etude]Diplôme de niveau bac+2`,
    ne.`[niveau_etude]Diplôme de niveau bac+3 ou bac+4`,
    ne.`[niveau_etude]Diplôme de niveau bac+5 ou plus`,

    -- Catégorie professionnelle
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

    -- Pouvoir d'achat
    pa2.`[pouvoir_achat]Pouvoir d'achat du RDB`,
    pa2.`[pouvoir_achat]Revenu disponible brut (RDB)`,

    -- Compte public
    cp2.`[compte_publique]depenses`,
    cp2.`[compte_publique]population`,
    cp2.`[compte_publique]euros_par_habitant`,

    -- Professionnels de santé
    ps.`[Spécialistes]EFFECTIF`,
    ps.`[Spécialistes]DENSITE /100 000 hab.`,
    ps.`[Généralistes et MEP]EFFECTIF`,
    ps.`[Généralistes et MEP]DENSITE /100 000 hab.`,
    ps.`[Auxiliaires médicaux]EFFECTIF`,
    ps.`[Auxiliaires médicaux]DENSITE /100 000 hab.`,
    ps.`[Sages-femmes]EFFECTIF`,
    ps.`[Sages-femmes]DENSITE /100 000 hab.`,
    ps.`[Dentistes et ODF]EFFECTIF`,
    ps.`[Dentistes et ODF]DENSITE /100 000 hab.`,
    ps.`[Laboratoires]EFFECTIF`,
    ps.`[Laboratoires]DENSITE /100 000 hab.`

FROM indicateurs base

LEFT JOIN age_moyen am 
    ON am.Code_departement = base.Code_departement 
    AND am.annee = base.annee

LEFT JOIN delinquance d 
    ON d.Code_departement = base.Code_departement 
    AND d.annee = base.annee

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
    ON ne.annee = base.annee

LEFT JOIN categorie_professionnelle cp 
    ON cp.annee = base.annee

LEFT JOIN pouvoir_achat pa2
    ON pa2.annee = base.annee

LEFT JOIN compte_publique cp2
    ON cp2.Code_departement = base.Code_departement
    AND cp2.annee = base.annee

LEFT JOIN professionnels_sante ps
    ON ps.Code_departement = base.Code_departement
    AND ps.annee = base.annee

WHERE
    am.`[age_moyen]0 à 4 ans` IS NOT NULL
    AND d.`[delinquance]nombre` IS NOT NULL
    AND d.`[delinquance]taux_pour_mille` IS NOT NULL
    AND tc.`[taux_chomage]Taux_moyen` IS NOT NULL
    AND es.`[equipement_sportif]nb_equipements` IS NOT NULL
    AND ec.`[etablissement_culturel]nombre_etablissements` IS NOT NULL;
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