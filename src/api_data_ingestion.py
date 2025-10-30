import requests
import pandas as pd
import credentials
import time
import os

# Get movie genres from TMDB API
def get_genres(headers):
    url = "https://api.themoviedb.org/3/genre/movie/list?language=en"

    genres = []

    response = requests.get(url, headers=headers)

    content = response.json()

    for genre in content['genres']:
        id = genre['id']
        name = genre['name']
        genres.append((id, name))

    genresDF = pd.DataFrame(genres, columns=['id', 'name'])

    return genresDF

# Get count of movies released between start_date and end_date in a specific country
def get_movies_count(start_date, end_date, country, headers):
    url_discover_movies = "https://api.themoviedb.org/3/discover/movie"
    movies_id = []

    # Première requête pour connaître le nombre total de pages
    response = requests.get(
        f"{url_discover_movies}?page=1&release_date.gte={start_date}&release_date.lte={end_date}&with_origin_country={country}",
        headers=headers
    )
    total_pages = response.json().get('total_pages', 1)

    # Boucle sur toutes les pages
    for page in range(1, total_pages + 1):
        try:
            url = f"{url_discover_movies}?page={page}&release_date.gte={start_date}&release_date.lte={end_date}&with_origin_country={country}"
            response = requests.get(url, headers=headers)
            content = response.json()
            
            for movie in content.get('results', []):
                movie_id = movie['id']
                movies_id.append(movie_id)
        
        except Exception as e:
            print(f"Erreur lors de la récupération des films à la page {page} : {e}")
        
        time.sleep(0.3)  # éviter d’être bloqué par l’API

    nb_movies = len(movies_id)
    print(f"✅ Nombre total de films trouvés : {nb_movies}")
    return movies_id

# Get movie IDs released between start_date and end_date
def get_movie_ids(start_date, headers):
    print(f"Recherche en cours des films sortis après le {start_date}...")
    url_discover_movies = "https://api.themoviedb.org/3/discover/movie"
    movies_id = []

    # Requête initiale pour connaître le nombre total de pages
    response = requests.get(
        f"{url_discover_movies}?page=1&release_date.gte={start_date}",
        headers=headers
    )
    total_pages = response.json().get("total_pages", 1)

    # Boucle sur toutes les pages
    for page in range(1, total_pages + 1):
        try:
            url = f"{url_discover_movies}?page={page}&release_date.gte={start_date}"
            response = requests.get(url, headers=headers)
            content = response.json()
            
            for movie in content.get('results', []):
                movies_id.append(movie['id'])
        
        except Exception as e:
            print(f"Erreur lors de la récupération des films à la page {page} : {e}")
        time.sleep(0.3)  # pour respecter les limites de l'API

    nb_movies = len(movies_id)
    print(f"✅ Nombre total de films trouvés : {nb_movies}")
    return movies_id

# Get detailed information for a specific movie by its ID ==> table Movies
def get_movie_details(movie_id, headers):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"

    response = requests.get(url, headers=headers)

    try:
        content = response.json()
        movie_details = {
            "movie_id": movie_id,
            "title": content.get("title"),
            "overview": content.get("overview"),
            "original_language": content.get("original_language"),
            "popularity": content.get("popularity"),
            "budget": content.get("budget"),
            "imdb_id": content.get("imdb_id"),
            "revenue": content.get("revenue"),
            "runtime": content.get("runtime"),
            "status": content.get("status"),
            "vote_average": content.get("vote_average"),
            "vote_count": content.get("vote_count"),
            "release_date": content.get("release_date")
        }

        print(f"Les données du film {movie_id} ont bien été récupérées.")
        return movie_details

    except Exception as e:
        print(f"Erreur lors de la récupération des données du film {movie_id} : {e}")
        return None

# Get ids of top 10 actors in a movie ==> table MovieActors
def get_movie_actor(movie_id, headers):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"

    response = requests.get(url, headers=headers)

    try:
        content = response.json()
        cast = content.get("cast", [])

        main_cast = [actor for actor in cast if actor.get("order", 0) < 10]

        movie_actor = [
            {"movie_id": movie_id, "actor_id": actor.get("id")} 
            for actor in main_cast
        ]

        print(f"Les acteurs du film {movie_id} ont bien été récupérées.")
        return movie_actor
    except Exception as e:
        print(f"Erreur lors de la récupération des acteurs du film {movie_id} : {e}")
        return None
    
# Get details of top 10 actors in a movie by its ID ==> table Actors
def get_actor_details(actor_id, headers):
    url = f"https://api.themoviedb.org/3/person/{actor_id}"

    response = requests.get(url, headers=headers)

    try:
        content = response.json()

        actor_details = {
            "actor_id": content.get("id"),
            "gender": content.get("gender"),
            "name": content.get("name"),
            "popularity": content.get("popularity")
        }

        print(f"Les données de l'acteur {actor_id} ont bien été récupérées.")
        return actor_details

    except Exception as e:
        print(f"Erreur lors de la récupération des données de l'acteur {actor_details.name} : {e}")
        return None

# Get production company details for a movie by its ID ==> table MovieProduction
def get_movie_production(movie_id, headers):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"

    response = requests.get(url, headers=headers)

    try:
        content = response.json()
        companies = content.get("production_companies", [])

        production_companies = [
            {"movie_id": movie_id, "production_id": company.get("id")} 
            for company in companies
        ]

        print(f"Les données de production du film {movie_id} ont bien été récupérées.")
        return production_companies

    except Exception as e:
        print(f"Erreur lors de la récupération des données dde production du film {movie_id} : {e}")
        return None

# Process and save movie data to CSV    
def process_movie_data():
    start_time = time.time()

    movie_ids = get_movie_ids("2025-10-01", "2025-10-31", headers)

    movie_details_list = []

    for i, movie_id in enumerate(movie_ids, start=1):
        details = get_movie_details(movie_id, headers)
        if details:
            movie_details_list.append(details)
        print(f"Progression : {i}/{len(movie_ids)} films traités.")
        time.sleep(0.5)  # Respecter les limites de l'API

    movie_df = pd.DataFrame(movie_details_list)
    movie_df.to_csv("data/movies_data.csv", index=False)
    print(f"✅ Fichier CSV sauvegardé : data/movies_data.csv ({len(movie_df)} films)")

    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Temps d'exécution : {execution_time:.4f} secondes")

# Process and save movie actors data to CSV
def process_movie_actors():
    start_time = time.time()

    # Récupérer la liste de acteurs des films présents dans la base de données (fichier movie_data.csv)
    file_path = "data/movies_data.csv"
    df = pd.read_csv(file_path, encoding="utf-8")

    movie_ids = df['id'].tolist()

    movie_actor = []

    for i, movie_id in enumerate(movie_ids, start=1):
        actor_list = get_movie_actor(movie_id, headers)
        print(f"Progression : {i}/{len(movie_ids)} films traités.")
        if actor_list:
            movie_actor.extend(actor_list)
        time.sleep(0.5)  # Respecter les limites de l'API
    
    movie_actor_df = pd.DataFrame(movie_actor)
    movie_actor_df.to_csv("data/movie_actors.csv", index=False)
    print(f"✅ Fichier CSV sauvegardé : data/movie_actor.csv ({len(movie_actor_df)} entrées)")

    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Temps d'exécution : {execution_time:.4f} secondes")

# Get movie genres by movie ID ==> table MovieGenres
def get_movie_genres(movie_id, headers):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"

    response = requests.get(url, headers=headers)

    try:
        content = response.json()
        genres = content.get("genres", [])

        movie_genres = [
            {"movie_id": movie_id, "genre_id": genre.get("id")} 
            for genre in genres
        ]

        print(f"Les genres du film {movie_id} ont bien été récupérées.")
        return movie_genres
    except Exception as e:
        print(f"Erreur lors de la récupération des genres du film {movie_id} : {e}")
        return None

# Get production company details by production ID ==> table Productions
def get_production_details(production_id, headers):
    
    url = f"https://api.themoviedb.org/3/company/{production_id}"

    response = requests.get(url, headers=headers)

    try:
        content = response.json()
        production_details = {
            "production_id": content.get("id"),
            "name": content.get("name"),
            "origin_country": content.get("origin_country")
        }
        print(f"Les données de la société de production {production_id} ont bien été récupérées.")
        return production_details
    except Exception as e:
        print(f"Erreur lors de la récupération des données de la société de production {production_id} : {e}")
        return None

# Process and save actor data to CSV
def process_actor_data():
    csv_path = "data/movie_actors.csv"
    df_movie_actors = pd.read_csv(csv_path)

    unique_actors = df_movie_actors['actor_id'].unique().tolist()

    print(f"Nombre d'acteurs distincts à traiter : {len(unique_actors)}")

    actor_details_list = []

    for i, actor_id in enumerate(unique_actors, start=1):
        details = get_actor_details(actor_id, headers)
        if details:
            actor_details_list.append(details)
        print(f"Progression : {i}/{len(unique_actors)} acteurs traités.")
        time.sleep(0.5)  # Respecter les limites de l'API

    actors_df = pd.DataFrame(actor_details_list)
    actors_df.to_csv("data/actors_data.csv", index=False)
    print(f"✅ Fichier CSV sauvegardé : data/actors_data.csv ({len(actors_df)} acteurs)")

def save_data_to_csv(csv_path, data):
    # 🔹 Toujours convertir en DataFrame d'abord
    if isinstance(data, list):
        data = pd.DataFrame(data)
    elif not isinstance(data, pd.DataFrame):
        raise TypeError("Le paramètre 'data' doit être une liste de dictionnaires ou un DataFrame.")

    # 🔹 Fusion ou création
    if os.path.exists(csv_path):
        df_existing = pd.read_csv(csv_path)
        df_combined = pd.concat([df_existing, data], ignore_index=True)
        df_combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"✅ Données ajoutées au fichier CSV : {csv_path} ({len(data)} nouvelles lignes)")
    else:
        data.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"✅ Fichier CSV créé et données ajoutées : {csv_path} ({len(data)} lignes)")

def process_movie_genres():
    csv_path = "data/movies_data.csv"
    df = pd.read_csv(csv_path)

    movie_ids = df['id'].tolist()

    movie_genre_df = []

    for i, movie_id in enumerate(movie_ids, start=1):
        genres = get_movie_genres(movie_id, headers)
        print(f"Progression : {i}/{len(movie_ids)} films traités.")
        if genres:
            movie_genre_df.extend(genres)
        time.sleep(0.5)  # Respecter les limites de l'API

    movie_genres_df = pd.DataFrame(movie_genre_df)
    movie_genres_df.to_csv("data/movie_genres.csv", index=False)
    print(f"✅ Fichier CSV sauvegardé : data/movie_genres.csv ({len(movie_genres_df)} entrées)")

# Define if an actor exists in the database
def actor_exists_in_db(actor_id, csv_path):
    df = pd.read_csv(csv_path)
    return actor_id in df['actor_id'].values

# Define if a genre exists in the database
def genre_exists_in_db(genre_id, csv_path):
    df = pd.read_csv(csv_path)
    return genre_id in df['id'].values

# Define if a production company exists in the database
def production_exists_in_db(production_id, csv_path):
    df = pd.read_csv(csv_path)
    return production_id in df['production_id'].values

def process_movie_productions():
    csv_path = "data/movies_data.csv"
    df = pd.read_csv(csv_path)

    movie_ids = df['id'].tolist()

    movie_productions_df = []

    for i, movie_id in enumerate(movie_ids, start=1):
        productions = get_movie_production(movie_id, headers)
        print(f"Progression : {i}/{len(movie_ids)} films traités.")
        if productions:
            movie_productions_df.extend(productions)
        time.sleep(0.5)  # Respecter les limites de l'API
    
    movie_productions_df = pd.DataFrame(movie_productions_df)
    movie_productions_df.to_csv("data/movie_productions.csv", index=False)
    print(f"✅ Fichier CSV sauvegardé : data/movie_productions.csv ({len(movie_productions_df)} entrées)")

def process_production_data():
    csv_path = "data/movie_productions.csv"
    df_movie_productions = pd.read_csv(csv_path)

    unique_productions = df_movie_productions['production_id'].unique().tolist()

    print(f"Nombre de productions distinctes à traiter : {len(unique_productions)}")

    production_details_list = []

    for i, production_id in enumerate(unique_productions, start=1):
        details = get_production_details(production_id, headers)
        if details:
            production_details_list.append(details)
        print(f"Progression : {i}/{len(unique_productions)} productions traités.")
        time.sleep(0.5)  # Respecter les limites de l'API

    production_df = pd.DataFrame(production_details_list)
    production_df.to_csv("data/productions_data.csv", index=False)
    print(f"✅ Fichier CSV sauvegardé : data/productions_data.csv ({len(production_df)} acteurs)")

if __name__ == "__main__":

    api_key = credentials.api_key

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    get_movie_ids("2025-01-01", headers)