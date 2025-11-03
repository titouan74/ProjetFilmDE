La base doit être régulièrement mise à jour avec les nouveaux films qui sorte

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
