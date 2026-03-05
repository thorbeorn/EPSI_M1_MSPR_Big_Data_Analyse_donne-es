-- phpMyAdmin SQL Dump
-- version 5.2.3
-- https://www.phpmyadmin.net/
--
-- Hôte : mariadb:3306
-- Généré le : jeu. 05 mars 2026 à 17:52
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
-- Structure de la table `age_moyen`
--

DROP TABLE IF EXISTS `age_moyen`;
CREATE TABLE `age_moyen` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[age_moyen]0 à 4 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]10 à 14 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]15 à 19 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]20 à 24 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]25 à 29 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]30 à 34 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]35 à 39 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]40 à 44 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]45 à 49 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]5 à 9 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]50 à 54 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]55 à 59 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]60 à 64 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]65 à 69 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]70 à 74 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]75 à 79 ans` bigint(20) DEFAULT NULL,
  `[age_moyen]80 ans et plus` bigint(20) DEFAULT NULL
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
-- Structure de la table `compte_publique`
--

DROP TABLE IF EXISTS `compte_publique`;
CREATE TABLE `compte_publique` (
  `Code_departement` text DEFAULT NULL,
  `annee` int(11) DEFAULT NULL,
  `[compte_publique]depenses` double DEFAULT NULL,
  `[compte_publique]population` bigint(20) DEFAULT NULL,
  `[compte_publique]euros_par_habitant` double DEFAULT NULL
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
-- Structure de la table `indicateurs`
--

DROP TABLE IF EXISTS `indicateurs`;
CREATE TABLE `indicateurs` (
  `annee` bigint(20) DEFAULT NULL,
  `Code_departement` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `niveau_etude`
--

DROP TABLE IF EXISTS `niveau_etude`;
CREATE TABLE `niveau_etude` (
  `index` bigint(20) DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[niveau_etude]Brevet des collèges` double DEFAULT NULL,
  `[niveau_etude]CAP, BEP ou équivalent` double DEFAULT NULL,
  `[niveau_etude]Diplôme de niveau bac+3 ou bac+4` double DEFAULT NULL,
  `[niveau_etude]Diplôme de niveau bac+2` double DEFAULT NULL,
  `[niveau_etude]Aucun diplôme, CEP` double DEFAULT NULL,
  `[niveau_etude]Baccalauréat ou équivalent` double DEFAULT NULL,
  `[niveau_etude]Diplôme de niveau bac+5 ou plus` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `pouvoir_achat`
--

DROP TABLE IF EXISTS `pouvoir_achat`;
CREATE TABLE `pouvoir_achat` (
  `annee` bigint(20) DEFAULT NULL,
  `[pouvoir_achat]Pouvoir d'achat du RDB` double DEFAULT NULL,
  `[pouvoir_achat]Revenu disponible brut (RDB)` double DEFAULT NULL
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
  `[president_sortant]nombre_voix` int(11) DEFAULT NULL,
  `[president_sortant]candidat` text DEFAULT NULL,
  `[president_sortant]famille_politique` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Structure de la table `professionnels_sante`
--

DROP TABLE IF EXISTS `professionnels_sante`;
CREATE TABLE `professionnels_sante` (
  `Code_departement` text DEFAULT NULL,
  `annee` bigint(20) DEFAULT NULL,
  `[Spécialistes]EFFECTIF` bigint(20) DEFAULT NULL,
  `[Spécialistes]DENSITE /100 000 hab.` double DEFAULT NULL,
  `[Généralistes et MEP]EFFECTIF` bigint(20) DEFAULT NULL,
  `[Généralistes et MEP]DENSITE /100 000 hab.` double DEFAULT NULL,
  `[Auxiliaires médicaux]EFFECTIF` bigint(20) DEFAULT NULL,
  `[Auxiliaires médicaux]DENSITE /100 000 hab.` double DEFAULT NULL,
  `[Sages-femmes]EFFECTIF` bigint(20) DEFAULT NULL,
  `[Sages-femmes]DENSITE /100 000 hab.` double DEFAULT NULL,
  `[Dentistes et ODF]EFFECTIF` double DEFAULT NULL,
  `[Dentistes et ODF]DENSITE /100 000 hab.` double DEFAULT NULL,
  `[Laboratoires]EFFECTIF` bigint(20) DEFAULT NULL,
  `[Laboratoires]DENSITE /100 000 hab.` double DEFAULT NULL
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
