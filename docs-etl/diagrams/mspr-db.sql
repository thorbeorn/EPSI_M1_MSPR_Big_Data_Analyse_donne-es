-- phpMyAdmin SQL Dump
-- version 5.2.3
-- https://www.phpmyadmin.net/
--
-- Hôte : mariadb:3306
-- Généré le : ven. 27 fév. 2026 à 13:16
-- Version du serveur : 11.8.6-MariaDB-ubu2404
-- Version de PHP : 8.3.26

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Base de données : `mspr-db`
--
CREATE DATABASE IF NOT EXISTS `mspr-db` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci;
USE `mspr-db`;

-- --------------------------------------------------------

--
-- Structure de la table `abstention_votant`
--

DROP TABLE IF EXISTS `abstention_votant`;
CREATE TABLE `abstention_votant` (
  `code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[abstention_votant]tour` text DEFAULT NULL,
  `[abstention_votant]inscrits` bigint(20) DEFAULT NULL,
  `[abstention_votant]abstentions` bigint(20) DEFAULT NULL,
  `[abstention_votant]blancs` double DEFAULT NULL,
  `[abstention_votant]nuls` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `age_moyen`
--

DROP TABLE IF EXISTS `age_moyen`;
CREATE TABLE `age_moyen` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[age_moyen]entre15et24` double DEFAULT NULL,
  `[age_moyen]entre25et54` double DEFAULT NULL,
  `[age_moyen]plus55` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `categorie_professionnelle`
--

DROP TABLE IF EXISTS `categorie_professionnelle`;
CREATE TABLE `categorie_professionnelle` (
  `annee` bigint(20) DEFAULT NULL,
  `[categorie_professionnelle] Agriculteurs` double DEFAULT NULL,
  `[categorie_professionnelle] Artisans, commerçants et patron` double DEFAULT NULL,
  `[categorie_professionnelle] Autres` double DEFAULT NULL,
  `[categorie_professionnelle] Cadres et professions supérieures` double DEFAULT NULL,
  `[categorie_professionnelle] Employés` double DEFAULT NULL,
  `[categorie_professionnelle] Employés peu qualifiés` double DEFAULT NULL,
  `[categorie_professionnelle] Employés qualifiés` double DEFAULT NULL,
  `[categorie_professionnelle] Ouvriers peu qualifiés` double DEFAULT NULL,
  `[categorie_professionnelle] Ouvriers qualifiés` double DEFAULT NULL,
  `[categorie_professionnelle] Professions intermédiaires` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `delinquance`
--

DROP TABLE IF EXISTS `delinquance`;
CREATE TABLE `delinquance` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[delinquance]nombre` bigint(20) DEFAULT NULL,
  `[delinquance]taux_pour_mille` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `equipement_sportif`
--

DROP TABLE IF EXISTS `equipement_sportif`;
CREATE TABLE `equipement_sportif` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[equipement_sportif]nb_equipements` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `etablissement_culturel`
--

DROP TABLE IF EXISTS `etablissement_culturel`;
CREATE TABLE `etablissement_culturel` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[etablissement_culturel]nombre_etablissements` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `indicateur`
--

DROP TABLE IF EXISTS `indicateur`;
CREATE TABLE `indicateur` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `niveau_etude`
--

DROP TABLE IF EXISTS `niveau_etude`;
CREATE TABLE `niveau_etude` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[niveau_etude]Aucun diplôme` double DEFAULT NULL,
  `[niveau_etude]BEPC, brevet élémentaire, brevet des collèges, DNB` double DEFAULT NULL,
  `[niveau_etude]Baccalauréat universitaire ou équivalent` double DEFAULT NULL,
  `[niveau_etude]Baccalauréat, brevet professionnel ou équivalent` double DEFAULT NULL,
  `[niveau_etude]CAP, BEP ou diplôme de niveau équivalent` double DEFAULT NULL,
  `[niveau_etude]CEP (certificat d’études primaires)` double DEFAULT NULL,
  `[niveau_etude]Diplôme d'études supérieures` double DEFAULT NULL,
  `[niveau_etude]Diplôme de niveau bac + 5 ou plus` double DEFAULT NULL,
  `[niveau_etude]Diplôme universitaire 2e ou 3e cycle` double DEFAULT NULL,
  `[niveau_etude]Enseignement supérieur de cycle court` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `population_active`
--

DROP TABLE IF EXISTS `population_active`;
CREATE TABLE `population_active` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `Statut_emploi` text DEFAULT NULL,
  `[population_active]entre15et24` double DEFAULT NULL,
  `[population_active]entre25et54` double DEFAULT NULL,
  `[population_active]entre55et64` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `pouvoir_achat`
--

DROP TABLE IF EXISTS `pouvoir_achat`;
CREATE TABLE `pouvoir_achat` (
  `annee` bigint(20) DEFAULT NULL,
  `[pouvoir_achat]pourcentage_annee_precedente` double DEFAULT NULL,
  `Pouvoir d’achat du revenu disponible brut` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `president_sortant`
--

DROP TABLE IF EXISTS `president_sortant`;
CREATE TABLE `president_sortant` (
  `code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[president_sortant]tour` text DEFAULT NULL,
  `[president_sortant]candidat` text DEFAULT NULL,
  `[president_sortant]famille_politique` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `revenu_moyen`
--

DROP TABLE IF EXISTS `revenu_moyen`;
CREATE TABLE `revenu_moyen` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[revenu_moyen]revenu_moyen_par_foyer` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `taux_chommage`
--

DROP TABLE IF EXISTS `taux_chommage`;
CREATE TABLE `taux_chommage` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[taux_chomage]Taux_moyen` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
