# Airflow - Orchestration

## 1) Préparation
Définir les variables d'environnement `API_KEY`, `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` et `POSTGRES_*` avant le lancement.

Exemple PowerShell :

```powershell
$env:API_KEY="remplacer_par_ta_cle_tmdb"
$env:AIRFLOW_USERNAME="admin"
$env:AIRFLOW_PASSWORD="admin"
$env:POSTGRES_HOST="host.docker.internal"
$env:POSTGRES_USER="postgres"
$env:POSTGRES_PASSWORD="postgres"
$env:POSTGRES_DB="postgres"
```

## 2) Lancer Airflow
Depuis le dossier `airflow` :

```powershell
docker compose up -d
```

Les metadata Airflow sont stockées dans PostgreSQL (`service airflow-db`).

Vérifier l'état des services :

```powershell
docker compose ps
```

Services attendus : `airflow-db` (healthy), `airflow-init` (terminé), `airflow-scheduler` (up), `airflow-webserver` (up).

Interface web : http://localhost:8088

Identifiants UI : `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD`

En cas de redémarrage propre :

```powershell
docker compose down
docker compose up -d
```

## 3) DAG disponible
- `ingestion_postgres_tmdb` : lance `src/ingestion/main_ingestion_postgre.py` - schedule = 1er lundi du mois 10h
- `db_and_model_update_dag.py` : execute la fonction `train_and_save_model` pour chaque modèle de ml - schedule ! tous les lundis 14h

## 4) Exécuter
Activer le DAG, puis déclencher un run manuel depuis l'UI.
