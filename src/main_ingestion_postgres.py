import requests
import pandas as pd
import credentials
import time
import api_data_ingestion as api
import os
import psycopg2
import db_insertion_postgres as db

if __name__ == "__main__":
    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
        dbname="movie_db",
        user="titouan",
        password="datascientest",
        host="34.244.249.48",
        port=5432
    )
    cursor = conn.cursor()

    # Configuration de l'API
    api_key = credentials.api_key
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    # Début du processus d'ingestion
    start_time = time.time()
    print("Démarrage de l'ingestion des données depuis l'API TMDB...")

    # Étape 1 : Récupérer les movie_ids depuis la base PostgreSQL
    cursor.execute("SELECT movie_id FROM movies")
    bdd_movie_ids = [row[0] for row in cursor.fetchall()]   
    print(f"Nombre de films dans la base de données : {len(bdd_movie_ids)}")

    # Étape 2 : Récupérer les ids de films à ingérer via l'API en définissant une période temporelle
    start_date = "2025-01-01"
    end_date = "2025-06-01"
    api_movie_ids = api.get_movie_ids(start_date, end_date, headers)

    # Étape 3 : Identifier les nouveaux films à ingérer
    new_movie_ids = list(set(api_movie_ids) - set(bdd_movie_ids))
    print(f"Nombre de nouveaux films à ingérer : {len(new_movie_ids)}")

    # ✅ Listes pour stocker les données
    new_movies_df = []
    new_people_df = []
    new_movie_people_df = []
    new_productions_df = []
    new_movie_productions_df = []
    new_genres_df = []        # <-- table parent genres
    new_movie_genres_df = []  # <-- table intermédiaire
    new_keywords_df = []
    new_movie_keywords_df = []

    count = 0
    new_count = 0

    # Boucle principale d'ingestion
    for movie_id in new_movie_ids:
        print(f"\nIngestion du film ID : {movie_id}")

        movie_details = api.get_movie_details(movie_id, headers)
        if not movie_details:
            print(f"Échec récupération film ID : {movie_id}")
            count += 1
            continue

        # Filtrage budget
        if movie_details.get("budget", 0) <= 50000:
            print(f"Film {movie_id} budget <= 50000, ignoré.")
            count += 1
            continue

        # 1️⃣ Films
        new_movies_df.append(movie_details)
        print(f"✅ Données film {movie_id} récupérées.")

        # 2️⃣ Genres
        movie_genres = api.get_movie_genres(movie_id, headers)
        if movie_genres:
            for genre in movie_genres:
                # Table intermédiaire
                new_movie_genres_df.append({"movie_id": movie_id, "genre_id": genre['id']})
                # Table parent
                if not any(g['genre_id'] == genre['id'] for g in new_genres_df):
                    new_genres_df.append({"genre_id": genre['id'], "genre_name": genre['name']})
            print(f"✅ Genres film {movie_id} récupérés.")

        # 3️⃣ Productions
        movie_productions = api.get_movie_production(movie_id, headers)
        if movie_productions:
            for prod in movie_productions:
                new_movie_productions_df.append({"movie_id": movie_id, "production_id": prod['production_id']})
                if not any(p['production_id'] == prod['production_id'] for p in new_productions_df):
                    prod_details = api.get_production_details(prod['production_id'], headers)
                    if prod_details:
                        new_productions_df.append(prod_details)
            print(f"✅ Productions film {movie_id} récupérées.")

        # 4️⃣ People
        movie_people = api.get_movie_people(movie_id, headers)
        if movie_people:
            for person in movie_people:
                new_movie_people_df.append({"movie_id": movie_id,
                                            "person_id": person['person_id'],
                                            "role_id": person['role_id']})
                if not any(p['person_id'] == person['person_id'] for p in new_people_df):
                    if not db.person_exists_in_db(person['person_id'], conn):
                        person_details = api.get_people_details(person['person_id'], headers)
                        if person_details:
                            new_people_df.append(person_details)
            print(f"✅ People film {movie_id} récupérés.")

        # 5️⃣ Keywords
        movie_keywords = api.get_movie_keywords(movie_id, headers)
        if movie_keywords:
            for kw in movie_keywords:
                new_movie_keywords_df.append({"movie_id": movie_id, "keyword_id": kw['keyword_id']})
                if not any(k['keyword_id'] == kw['keyword_id'] for k in new_keywords_df):
                    kw_details = api.get_keywords_details(kw['keyword_id'], headers)
                    if kw_details:
                        new_keywords_df.append(kw_details)
            print(f"✅ Keywords film {movie_id} récupérés.")

        count += 1
        new_count += 1
        print(f"Progression : {count}/{len(new_movie_ids)} films.")

        # Sauvegarde partielle tous les 10 films
        if count % 10 == 0:
            db.insert_genres_to_db(conn, new_genres_df); new_genres_df.clear()
            db.insert_movies_to_db(conn, new_movies_df); new_movies_df.clear()
            db.insert_people_to_db(conn, new_people_df); new_people_df.clear()
            db.insert_productions_to_db(conn, new_productions_df); new_productions_df.clear()
            db.insert_keywords_to_db(conn, new_keywords_df); new_keywords_df.clear()
            db.insert_movie_genres_to_db(conn, new_movie_genres_df); new_movie_genres_df.clear()
            db.insert_movie_people_to_db(conn, new_movie_people_df); new_movie_people_df.clear()
            db.insert_movie_productions_to_db(conn, new_movie_productions_df); new_movie_productions_df.clear()
            db.insert_movie_keywords_to_db(conn, new_movie_keywords_df); new_movie_keywords_df.clear()
            print("💾 Sauvegarde partielle effectuée.")

    # Insertion finale dans la base
    db.insert_genres_to_db(conn, new_genres_df)
    db.insert_movies_to_db(conn, new_movies_df)
    db.insert_people_to_db(conn, new_people_df)
    db.insert_productions_to_db(conn, new_productions_df)
    db.insert_keywords_to_db(conn, new_keywords_df)
    db.insert_movie_genres_to_db(conn, new_movie_genres_df)
    db.insert_movie_people_to_db(conn, new_movie_people_df)
    db.insert_movie_productions_to_db(conn, new_movie_productions_df)
    db.insert_movie_keywords_to_db(conn, new_movie_keywords_df)

    print(f"\n✅ Ingestion terminée : {new_count} films ajoutés.")

    conn.commit()
    cursor.close()
    conn.close()

    end_time = time.time()
    print(f"🕦 Temps d'exécution : {(end_time - start_time)/60:.2f} minutes")
