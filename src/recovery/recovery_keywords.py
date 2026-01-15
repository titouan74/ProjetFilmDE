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

    # Récupérer les keyword ids pour chaque film 
    movie_keywords_path = "data/movie_keywords.csv"
    movie_keywords_df = []

    movie_count = 1
    total_movies = len(movie_ids)

    for movie_id in movie_ids:
        print(f"➡️ Récupération des keywords pour le film ID : {movie_id}")
        movie_keywords = api.get_movie_keywords(movie_id, headers)
        if movie_keywords:
            movie_keywords_df.extend(movie_keywords)
            print(f"Récupérés {len(movie_keywords)} keywords pour le film {movie_id}.")
            print(f"⏳ Progression : {movie_count}/{total_movies} films traités.")
        else:
            print(f"⚠️ Aucun mot-clé trouvé pour le film ID : {movie_id}.")
        movie_count += 1
    print(f"ℹ️ Traitement terminé : {len(movie_keywords_df)} entrées récupérées.")

    # Sauvegarder les données des keywords dans un CSV
    api.save_data_to_csv(movie_keywords_path, movie_keywords_df)

    # Récupérer le détail des keywords
    keywords_path = "data/keywords_data.csv"
    keywords_df = []

    keyword_ids = pd.read_csv(movie_keywords_path)['keyword_id'].unique().tolist()
    total_keywords = len(keyword_ids)
    keyword_count = 1

    for keyword_id in keyword_ids:
        keyword_details = api.get_keywords_details(keyword_id, headers)
        if keyword_details:
            keywords_df.append(keyword_details)
            print(f"ℹ️ Les informations ont bien été récupérées pour le keyword ID : {keyword_id}")
            print(f"⏳ Progression : {keyword_count}/{total_keywords} keywords traités.")
            keyword_count += 1
        else:
            print(f"⚠️ Aucune information trouvée pour le keyword ID : {keyword_id}")

    print(f"✅ Traitement terminé : {len(keywords_df)} keywords récupérés.")

    # Sauvegarder les données des keywords dans un CSV
    api.save_data_to_csv(keywords_path, keywords_df)

    end_time = time.time()

    execution_time = (end_time - start_time)/60
    print(f"🕦 Temps d'exécution : {execution_time:.2f} minutes")