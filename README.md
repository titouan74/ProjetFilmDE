Test demo

La base doit être régulièrement mise à jour avec les nouveaux films qui sorte

CREDENTIALS
Pour ingérer des données de l'API TheMovieDB, il faut inclure dans le dossier src/ingestion un fichier credentials.py qui contient la clé.
Exemple : api_key = "mY@pIKey!"

CREATION DE LA BASE DE DONNEES
1. Pour créer une nouvelle base à partir du docker-compose.yaml, se placer dans le dosser src/init et lancer la commande suivante : 'docker compose up' ou 'docker-compose up' selon la distribution
2. Installer les librairies nécessaires au fonctionnement de la base depuis le fichier requirements.txt: pip3 install -r requirements.txt (recommandé de le faire dans un environnement virtuel)
3. Lancer le script 'create_tables.py'
4. La base peut-être alimentées par les données présentes dans les fichiers csv (/data) en lançant le script 'db_init.py'


INGESTION DES NOUVELLES DONNÉES DEPUIS L'API (main_ingestion.py):
1. Récupérer les ids des nouveaux films et les comparer avec les ids de la base :
    - Si l'id est déjà dans la base : RIEN
    - Si l'id n'est pas dans la base --> On lance l'ingestion
1. Ajouter des données dans la table Movie à partir du movie_id
2. Récupérer la liste des acteurs à partir du movie_id
    --> Si l'acteur est dans la base : RIEN
    --> Si l'acteur n'est pas dans la base : Ajout à la table Actors
3. Récupérer la liste des genres du films et ajouter
    3.1. Ajouter le couple (movie_id, genre_id) à la table MovieGenre
    3.1. Comparer les genre_id à la table Genres
        --> Si le genre est dans la table : RIEN
        --> Si le genre n'est pas dans la table : mise à jour de la table Genres depuis l'API

En plus de l'ingestion des nouveaux films, certaines données doivent être mise à jour régulièrement pour l'ensemble de la base:
    - Nombre de votes
    - Note moyenne
    - Popularité (movie)
    - Popularité (acteur)
    - Revenus ?

MISE A JOUR DE LA BASE EXISTANTE
1. Récupère les movie_ids de la base
2. On récupère via la route Movie / Details les valeurs à jours (votes, note)
3. On insert les nouvelles valeurs dans la base

DESCRIPTION DES FICHIERS:
- `recovery_keywords` et `recovery_people` --> utilisés une fois pour récupérer des données sur des films existants dans la base (suite au changement de schéma de la base)
- `api_data_ingestion` --> fonctions pour l'ingestion des données depuis l'API TheMovieDB
- `main_ingestion_csv` --> processus d'ingestion des nouveaux films avec sauvegarde au format csv
- `main_ingestion_postgre` --> processus d'ingestion des nouveaux films avec sauvegarde dans la base postgre
- `db_instertion_csv` --> fonctions pour l'insertion des données dans des fichiers csv
- `db_instertion_postgre` --> fonctions pour l'insertion des données dans la base postgre

AIRFLOW (ORCHESTRATION)
- Le dossier des DAG est placé dans `airflow/dags` à la racine du projet.
- Une stack Airflow minimale est disponible dans `airflow/docker-compose.yml`.
- Définir `API_KEY` dans l'environnement shell avant lancement (ou via un fichier `.env` local).
- Depuis le dossier `airflow`, lancer : `docker compose up -d`.
- UI Airflow : http://localhost:8088
- DAGs créés : `ingestion_postgres_tmdb` (exécute `src/ingestion/main_ingestion_postgre.py`).

CONTINUOUS INTEGRATION
Fonctionnement standard (https://gitlab.com/ottinger74-group/ProjetFilmDE):
1. Push GitHub → Workflow GitHub Actions "Mirror to GitLab" (via personal_access_tokens sur Gitlab et repository_secrets dans Github) → Push automatique sur GitLab
2. Le runner partagé GitLab détecte le push et lance le pipeline défini dans .gitlab-ci.yml
3. Un conteneur PostgreSQL 15 est démarré en tant que service avec la base movie_db et l'utilisateur titouan
4. Le dump SQL movie_db_dump.sql est importé pour peupler la base de données
5. Le serveur FastAPI est lancé en arrière-plan via uvicorn sur le port 8000
6. Le job test exécute les tests définis dans test_assert.py via pytest
7. Les résultats (passed/failed) sont consultables dans l'onglet CI/CD → Jobs de GitLab, avec le détail des requêtes HTTP effectuées sur chaque endpoint testé

Mise à jour de la base de données de test :
1. Lancer le script dump.py qui génère un dump de la base PostgreSQL locale et le place dans src/api/movie_db_dump.sql
2. Si les identifiants de la base locale ont changé, mettre à jour POSTGRES_USER et POSTGRES_PASSWORD dans la section services du .gitlab-ci.yml
3. Pusher sur GitHub les fichiers modifiés (movie_db_dump.sql et/ou .gitlab-ci.yml) — le mirror se chargera de synchroniser automatiquement sur GitLab et déclenchera le pipeline

MONITORING (PROMETHEUS + GRAFANA)
Objectif: superviser l'API FastAPI et PostgreSQL.

1. Vérifier/compléter le fichier `src/api/.env` (voir `src/api/.env.example`):
    - `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`
    - `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` (optionnel)
2. Depuis `src/api`, lancer la stack:
    - `docker compose up -d --build`
3. Accès aux interfaces:
    - API: http://localhost:8000
    - Metrics API (Prometheus scrape): http://localhost:8000/metrics
    - Prometheus: http://localhost:9090
    - Grafana: http://localhost:3000
4. Dashboard Grafana provisionné automatiquement:
    - `Movie API + PostgreSQL Monitoring`

Notes:
- Le service `postgres-exporter` expose les métriques PostgreSQL sur le port 9187.
- Si PostgreSQL tourne dans une autre stack Docker locale (ex: `src/init/docker-compose.yaml`), `POSTGRES_HOST=host.docker.internal` fonctionne généralement sur Windows.

Suivi de l'ingestion PostgreSQL (PromQL / Grafana):
- Débit d'insertions (lignes/s):
    - `sum(rate(pg_stat_database_tup_inserted{datname="movie_db"}[5m]))`
- Débit de mises à jour (lignes/s):
    - `sum(rate(pg_stat_database_tup_updated{datname="movie_db"}[5m]))`
- Volume inséré sur 1 heure:
    - `sum(increase(pg_stat_database_tup_inserted{datname="movie_db"}[1h]))`

Le dashboard `Movie API + PostgreSQL Monitoring` contient maintenant ces 3 panneaux dédiés à l'ingestion.
