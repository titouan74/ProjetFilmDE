import pandas as pd
import ingestion.credentials as credentials
import time
import ingestion.api_data_ingestion as api

if __name__ == "__main__":

    start_time = time.time()
    print("Démarrage de la récupération des données manquantes depuis l'API TMDB...")

    api_key = credentials.api_key
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    # Charger les movie_ids existants depuis le CSV des films
    movies = pd.read_csv("data/movies_data.csv")
    movie_ids = movies['movie_id'].unique().tolist()
    #movie_ids = movie_ids[:10]  # Limiter à 10 pour des raisons de test

    # Récupérer les people ids pour chaque film 
    movie_people_path = "data/movie_people.csv"
    movie_people_df = []
    movie_count = 1
    total_movies = len(movie_ids)

    for movie_id in movie_ids:
        print(f"➡️ Récupération des people pour le film ID : {movie_id}")
        movie_people = api.get_movie_people(movie_id, headers)
        if movie_people:
            movie_people_df.extend(movie_people)
            print(f"Récupérés {len(movie_people)} people pour le film {movie_id}.")
            print(f"⏳ Progression : {movie_count}/{total_movies} films traités.")
        else:
            print(f"⚠️ Aucun people trouvé pour le film ID : {movie_id}.")
        movie_count += 1
    print(f"ℹ️ Traitement terminé : {len(movie_people_df)} entrées récupérées.")

    # Sauvegarder les données des people dans un CSV
    api.save_data_to_csv(movie_people_path, movie_people_df)

    # Récupérer le détail des people
    people_path = "data/people_data.csv"
    people_df = []
    people_ids = pd.read_csv(movie_people_path)['people_id'].unique().tolist()
    total_people = len(people_ids)
    people_count = 1

    for people_id in people_ids:
        people_details = api.get_people_details(people_id, headers)
        if people_details:
            people_df.append(people_details)
            print(f"ℹ️ Les informations ont bien été récupérées pour le people ID : {people_id}")
            print(f"⏳ Progression : {people_count}/{total_people} people traités.")
            people_count += 1
        else:
            print(f"⚠️ Aucune information trouvée pour le people ID : {people_id}")

    # Sauvegarder les données des people dans un CSV
    api.save_data_to_csv(people_path, people_df)
    print(f"✅ Traitement terminé : {len(people_df)} people récupérés.")

    end_time = time.time()

    execution_time = (end_time - start_time)/60
    print(f"🕦 Temps d'exécution : {execution_time:.2f} minutes")