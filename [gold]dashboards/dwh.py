import pandas as pd
from sqlalchemy import create_engine

def create_gold_all_indicator_df():
    # Création du moteur de connexion à la base MySQL
    engine = create_engine(
        "mysql+pymysql://mspr-user:z9k5RYgeDr3457TV33tY2eLPgd36XE5y88LAcCpz@localhost:3306/mspr-db"
    )

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
        av.`blancs_total` AS `[abstention_votant]blancs`,
        av.`nuls_total` AS `[abstention_votant]nuls`

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
            SUM(`[abstention_votant]blancs`) AS blancs_total,
            SUM(`[abstention_votant]nuls`) AS nuls_total
        FROM abstention_votant
        GROUP BY code_departement, annee
    ) av
    ON av.code_departement = base.Code_departement
    AND av.annee = base.annee;
    """

    df = pd.read_sql(query, engine)
    print(df)

create_gold_all_indicator_df()