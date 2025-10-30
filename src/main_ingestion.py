import requests
import pandas as pd
import credentials
import time
import api_data_ingestion as api
import os

if __name__ == "__main__":
    start_time = time.time()
    print("Démarrage de l'ingestion des données depuis l'API TMDB...")

    api_key = credentials.api_key

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    # Étape 1 : Récupérer les movie_ids depuis le CSV (à terme, depuis la BDD)
    movie_path = "data/movies_data.csv"
    if os.path.exists(movie_path):
        movie_df = pd.read_csv(movie_path)
        bdd_movie_ids = movie_df['movie_id'].unique().tolist()
    else:
        bdd_movie_ids = []
    
    print(f"Nombre de films dans la base de données : {len(bdd_movie_ids)}")

    # Étape 2 : Récupérer des films via l'API en définissant une période temporelle
    start_date = "2025-01-01"
    api_movie_ids = api.get_movie_ids(start_date, headers)

    # Étape 3 : Identifier les movie_ids qui ne sont pas encore dans la BDD
    new_movie_ids = list(set(api_movie_ids) - set(bdd_movie_ids))
    print(f"Nombre de nouveaux films à ingérer : {len(new_movie_ids)}")

    # Étape 4 : Ingestion des données pour chaque nouveau movie_id
    movies_path = "data/movies_data.csv"
    actors_path = "data/actors_data.csv"
    production_path = "data/productions_data.csv"
    movie_genres_path = "data/movie_genres.csv"
    movie_actors_path = "data/movie_actors.csv"
    movie_productions_path = "data/movie_productions.csv"

    new_movies_df = []
    new_actors_df = []
    new_movie_actors_df = []
    new_movie_productions_df = []
    new_productions_df = []
    new_movie_genres_df = []

    count = 0

    for movie_id in new_movie_ids:
        print(f"\nIngestion des données pour le film ID : {movie_id}")

        # Récupérer et stocker les détails du film
        movie_details = api.get_movie_details(movie_id, headers)
        if movie_details:
            new_movies_df.append(movie_details)
            print(f"Le film {movie_id} a bien été récupéré.")
        else:
            print(f"Échec de la récupération des détails pour le film ID : {movie_id}")
            continue

        # Récupérer et stocker les acteurs du film puis ajouter les nouveaux acteurs à la BDD
        movie_actor = api.get_movie_actor(movie_id, headers)
        if movie_actor:
            new_movie_actors_df.extend(movie_actor)
            for actor in movie_actor:
                if any(a['actor_id'] == actor['actor_id'] for a in new_actors_df):
                    continue  # Acteur déjà ajouté au cours de cette ingestion
                elif os.path.exists(actors_path) and api.actor_exists_in_db(actor['actor_id'], actors_path) :
                    continue  # Acteur déjà présent dans la BDD
                else:
                    actor_details = api.get_actor_details(actor['actor_id'], headers)
                    if actor_details:
                        new_actors_df.append(actor_details)
                time.sleep(0.25)  # Respecter les limites de l'API
        else:
            print(f"Aucun acteur trouvé pour le film ID : {movie_id}")

        # Récupérer et stocker les genres du film
        movie_genre = api.get_movie_genres(movie_id, headers)
        if movie_genre:
            new_movie_genres_df.extend(movie_genre)
            time.sleep(0.25)  # Respecter les limites de l'API
        else:
            print(f"Aucun genre trouvé pour le film ID : {movie_id}")
        
        # Récupérer et stocker les productions du film
        movie_production = api.get_movie_production(movie_id, headers)
        if movie_production:
            new_movie_productions_df.extend(movie_production)
            for production in movie_production:
                if any(p['production_id'] == production['production_id'] for p in new_productions_df):
                    continue  # Production déjà ajoutée au cours de cette ingestion
                elif os.path.exists(production_path) and api.production_exists_in_db(production['production_id'], production_path):
                    continue  # Production déjà présente dans la BDD
                else:
                    production_details = api.get_production_details(production['production_id'], headers)
                    if production_details:
                        new_productions_df.append(production_details)
                time.sleep(0.25)  # Respecter les limites de l'API
        
        count += 1
        print(f"Ingestion terminée pour le film ID : {movie_id}")
        print(f"Progression : {count}/{len(new_movie_ids)} films.")

    # Étape 5 : Enregistrement des données collectées dans les fichiers CSV
    api.save_data_to_csv(movies_path, new_movies_df)
    api.save_data_to_csv(actors_path, new_actors_df)
    api.save_data_to_csv(movie_actors_path, new_movie_actors_df)
    api.save_data_to_csv(movie_productions_path, new_movie_productions_df)
    api.save_data_to_csv(production_path, new_productions_df)
    api.save_data_to_csv(movie_genres_path, new_movie_genres_df)
    print("\n✅ Ingestion des données terminée. Données sauvegardées avec succès dans les fichiers CSV !")
    
    end_time = time.time()

    execution_time = (end_time - start_time)/60
    print(f"🕦 Temps d'exécution : {execution_time:.2f} minutes")