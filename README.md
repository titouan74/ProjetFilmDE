La base doit être régulièrement mise à jour avec les nouveaux films qui sorte

CREDENTIALS
Pour ingérer des données de l'API TheMovieDB, il faut inclure dans le dossier src/ingestion un fichier credentials.py qui contient la clé.
Exemple : api_key = "mY@pIKey!"

INGESTION DES NOUVELLES DONNÉES (main_ingestion.py):
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
- 'recovery_keywords' et 'recovery_people' --> utilisés une fois pour récupérer des données sur des films existants dans la base (suite au changement de schéma de la base)
- 'api_data_ingestion' --> fonctions pour l'ingestion des données depuis l'API TheMovieDB
- 'main_ingestion_csv' --> processus d'ingestion des nouveaux films avec sauvegarde au format csv
- 'main_ingestion_postgre' --> processus d'ingestion des nouveaux films avec sauvegarde dans la base postgre
- 'db_instertion_csv' --> fonctions pour l'insertion des données dans des fichiers csv
- 'db_instertion_postgre' --> fonctions pour l'insertion des données dans la base postgre