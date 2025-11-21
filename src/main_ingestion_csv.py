import requests
import pandas as pd
import credentials
import time
import api_data_ingestion as api
import db_insertion_csv as csv
import os

os.makedirs("data", exist_ok=True) #si le dossier "data" n'existe pas préalablement au main

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
        try:
            movie_df = pd.read_csv(movie_path)
            # Vérifie si la colonne 'movie_id' existe et contient au moins un élément
            if 'movie_id' in movie_df.columns and not movie_df['movie_id'].empty:
                bdd_movie_ids = movie_df['movie_id'].unique().tolist()
            else:
                bdd_movie_ids = []
        except pd.errors.EmptyDataError:
            bdd_movie_ids = []
    else:
        bdd_movie_ids = []
    
    print(f"Nombre de films dans la base de données : {len(bdd_movie_ids)}")

    # Étape 2 : Récupérer des films via l'API en définissant une période temporelle
    start_date = "2024-12-01"
    api_movie_ids = api.get_movie_ids(start_date, headers)

    # Étape 3 : Identifier les movie_ids qui ne sont pas encore dans la BDD
    new_movie_ids = list(set(api_movie_ids) - set(bdd_movie_ids))
    print(f"Nombre de nouveaux films à ingérer : {len(new_movie_ids)}")

    # Étape 4 : Ingestion des données pour chaque nouveau movie_id
    movies_path = "data/movies_data.csv"
    people_path = "data/people_data.csv"
    movie_people_path = "data/movie_people.csv"
    production_path = "data/productions_data.csv"
    movie_productions_path = "data/movie_productions.csv"
    movie_genres_path = "data/movie_genres.csv"
    movie_keywords_path = "data/movie_keywords.csv"
    keywords_path = "data/keywords_data.csv"

    new_movies_df = []
    new_people_df = []
    new_movie_people_df = []
    new_productions_df = []
    new_movie_productions_df = []
    new_movie_genres_df = []
    new_movie_keywords_df = []
    new_keywords_df = []

    count = 0

    for movie_id in new_movie_ids:
        print(f"\nIngestion des données pour le film ID : {movie_id}")
        time.sleep(0.5) #plusieurs fonctions dans la même boucle --> ajout d'une pause un peu plus longue

        # Récupérer et stocker les détails du film
        movie_details = api.get_movie_details(movie_id, headers)
        if movie_details:
            new_movies_df.append(movie_details)
            print(f"Le film {movie_id} a bien été récupéré.")
        else:
            print(f"Échec de la récupération des détails pour le film ID : {movie_id}")
            continue

        # Récupérer et stocker les personnes clées du film puis les ajouter à la BDD
        movie_people = api.get_movie_people(movie_id, headers)
        if movie_people:
            new_movie_people_df.extend(movie_people)
            for person in movie_people:
                pid = person['people_id']
                if any(p['person_id'] == pid for p in new_people_df):
                    continue
                elif os.path.exists(people_path) and api.people_exists_in_db(pid, people_path):
                    continue
                else:
                    details = api.get_people_details(pid, headers)
                    if details:
                        new_people_df.append(details)
                time.sleep(0.25)
        else:
            print(f"⚠️ Aucun people trouvé pour le film {movie_id}")

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
        
        # Récupérer et stocker les keywords du film
        movie_keywords = api.get_movie_keywords(movie_id, headers)
        if movie_keywords:
            new_movie_keywords_df.extend(movie_keywords)
            for keyword in movie_keywords:
                kid = keyword['keyword_id']
                if any(k['keyword_id'] == kid for k in new_keywords_df):
                    continue
                elif os.path.exists(keywords_path) and api.keyword_exists_in_db(kid, keywords_path):
                    continue
                else:
                    details = api.get_keywords_details(kid, headers)
                    if details:
                        new_keywords_df.append(details)
                time.sleep(0.25)
        
        count += 1
        print(f"Ingestion terminée pour le film ID : {movie_id}")
        print(f"Progression : {count}/{len(new_movie_ids)} films.")

        # Sauvegarde partielle pour éviter la perte de données en cas de rupture de connexion
        if count % 100 == 0:
            api.save_data_to_csv(movies_path, new_movies_df); new_movies_df.clear()
            api.save_data_to_csv(people_path, new_people_df); new_people_df.clear()
            api.save_data_to_csv(movie_people_path, new_movie_people_df); new_movie_people_df.clear()
            api.save_data_to_csv(production_path, new_productions_df); new_productions_df.clear()
            api.save_data_to_csv(movie_productions_path, new_movie_productions_df); new_movie_productions_df.clear()
            api.save_data_to_csv(movie_genres_path, new_movie_genres_df); new_movie_genres_df.clear()
            api.save_data_to_csv(movie_keywords_path, new_movie_keywords_df); new_movie_keywords_df.clear()
            api.save_data_to_csv(keywords_path, new_keywords_df); new_keywords_df.clear()
            print(f"✅ Sauvegarde intermédiaire terminée après {count} films.")

    # Étape 5 : Enregistrement finale des données collectées dans les fichiers CSV
    api.save_data_to_csv(movies_path, new_movies_df)
    api.save_data_to_csv(people_path, new_people_df)
    api.save_data_to_csv(movie_people_path, new_movie_people_df)
    api.save_data_to_csv(production_path, new_productions_df)
    api.save_data_to_csv(movie_productions_path, new_movie_productions_df)
    api.save_data_to_csv(movie_genres_path, new_movie_genres_df)
    api.save_data_to_csv(movie_keywords_path, new_movie_keywords_df)
    api.save_data_to_csv(keywords_path, new_keywords_df)
    print("\n✅ Ingestion des données terminée. Données sauvegardées avec succès dans les fichiers CSV !")
    
    end_time = time.time()

    execution_time = (end_time - start_time)/60
    print(f"🕦 Temps d'exécution : {execution_time:.2f} minutes")