# Diagramme Entité-Relation (ERD) avec Azimutt
## Description

Ce projet contient un Modèle Conceptuel de Données (MCD) réalisé avec Azimutt.

## Azimutt

[**Azimutt**](https://azimutt.app/) est un outil de visualisation et d'analyse de bases de données qui permet de créer des diagrammes entités-relations (ERD), facilitant ainsi la modélisation des bases de données relationnelles. Il est particulièrement adapté pour la conception et la compréhension des structures complexes.

### Fonctionnalités principales d'Azimutt :

- Création de diagrammes à partir de fichiers SQL ou de connexions à des bases de données.
- Mise en évidence des relations entre les tables.
- Interface intuitive pour la navigation et l'organisation des tables.

## Contenu du projet

- **Fichier JSON** : Le fichier JSON contient la définition des tables, relations et attributs du MCD. Ce fichier a été généré et exporté depuis Azimutt.
- **Ce README** : Ce document explique le contexte du projet et l'outil utilisé pour créer le MCD.

## Utilisation

Pour visualiser ou modifier le MCD :

1. Exporter le schema des tables en sql 
2. Ouvrez [Azimutt](https://azimutt.app/new).
3. Importez le fichier SQL Exporter.
4. Naviguez dans les tables et relations pour explorer le modèle.

## Avantages d'Azimutt pour ce projet

- Simplification de la modélisation des données.
- Visualisation claire des relations entre les entités.
- Flexibilité pour ajouter, modifier ou supprimer des éléments du modèle.

## Exporter la structure (sans données) depuis phpMyAdmin

1. Connectez-vous à phpMyAdmin.
2. Dans le menu de gauche, cliquez sur votre **base de données**.
3. En haut, cliquez sur l’onglet **Exporter**.
4. Choisissez la méthode **Personnalisée** (Custom).
5. Dans la section **Format**, laissez **SQL**.
6. Faites défiler jusqu’à **Options spécifiques au format** :

   * **Décochez** : *Données*
   * **Cochez uniquement** : *Structure*
     (ou sélectionnez **Structure** dans *Objet à exporter* selon la version)
7. (Optionnel mais recommandé)

   * Cochez **Ajouter IF NOT EXISTS**
   * Cochez **Ajouter AUTO_INCREMENT**
   * Incluez **Triggers**, **Routines**, **Événements** si présents.
8. Cliquez sur **Exécuter**.

Un fichier `.sql` contenant **uniquement la structure** (tables, index, contraintes, etc.) sera téléchargé.