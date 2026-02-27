# README.md

## 📦 Stack Docker – MariaDB, phpMyAdmin et MinIO

Ce projet fournit un environnement local basé sur Docker comprenant :

* **MariaDB 11** – Base de données relationnelle
* **phpMyAdmin** – Interface web de gestion de la base
* **MinIO** – Stockage d’objets compatible S3

---

## 🚀 Prérequis

* Docker installé
* Docker Compose installé
* Ports disponibles :

  * 3306 (MariaDB)
  * 8080 (phpMyAdmin)
  * 9000 (API MinIO)
  * 9001 (Console MinIO)

---

## 📁 Structure du projet

```
.
├── docker-compose.yml
├── initdb/          # Scripts SQL exécutés au démarrage de MariaDB
└── minio-data/      # Données persistantes MinIO
```

---

## ▶️ Lancer les services

Dans le dossier du projet :

```bash
docker compose up -d
```

Vérifier que les conteneurs tournent :

```bash
docker ps
```

Arrêter les services :

```bash
docker compose down
```

---

## 🗄️ Accès aux services

### MariaDB

* Host : `localhost`
* Port : `3306`
* Base : `mspr-db`
* User : `mspr-user`
* Password : défini dans `docker-compose.yml`

### phpMyAdmin

Accès via navigateur :

```
http://localhost:8080
```

Paramètres :

* Serveur : `mariadb`
* User : `mspr-user`
* Password : voir configuration

---

### MinIO

#### API S3

```
http://localhost:9000
```

#### Console Web

```
http://localhost:9001
```

Identifiants :

* User : `mspr-admin`
* Password : défini dans `docker-compose.yml`

---

## 🧩 Initialisation de la base

Tous les fichiers `.sql` placés dans :

```
./initdb
```

seront exécutés automatiquement lors du premier démarrage du conteneur MariaDB.

---

## 💾 Persistance des données

* **MariaDB** : données internes au conteneur
* **MinIO** : stockées dans `./minio-data`

---

## ⚠️ Sécurité

Ce projet est configuré pour un **environnement de développement local**.
Pour un usage en production :

* Utiliser des variables d’environnement sécurisées
* Ne pas exposer les ports publiquement
* Ajouter HTTPS et une gestion des secrets

---

## 🛠️ Commandes utiles

Rebuild complet :

```bash
docker compose down -v
docker compose up -d --build
```

Voir les logs :

```bash
docker compose logs -f
```