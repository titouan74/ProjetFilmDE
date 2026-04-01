import requests
import pandas as pd
import time
import ingestion.api_data_ingestion as api
import os
import sys
import init.db_insertion_postgres as db

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import connect_to_db

if __name__ == "__main__":
    # Connexion à la base de données PostgreSQL
    engine = connect_to_db()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Configuration de l'API
    api_key = os.getenv("API_KEY")
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    # Paramètres de mise à jour
    moviesUpdate = False
    peopleUpdate = False
    genresUpdate = True

    movie_query = f"""
        SELECT movie_id 
        FROM movies 
        WHERE budget > 50000
            AND revenue > 50000
            AND vote_count > 100
            AND release_date > '2000-01-01'
    """
    people_query = f"""
        SELECT mp.person_id 
        FROM movie_people mp
        WHERE movie_id IN (
            SELECT m.movie_id 
            FROM movies m
            WHERE m.budget > 50000
                AND m.revenue > 50000
                AND m.vote_count > 100
                AND m.release_date > '2000-01-01'
        )GROUP BY person_id
    """

    # Début du processus d'ingestion
    start_time = time.time()
    print("Démarrage de l'ingestion des données depuis l'API TMDB avec les paramètres suivants :")
    print(f"  - Mise à jour des films : {moviesUpdate}")
    print(f"  - Mise à jour des personnes : {peopleUpdate}")
    print(f"  - Mise à jour des genres : {genresUpdate}")

    if genresUpdate:
        # Récupérer tous les genres depuis l'API
        all_genres_df = api.get_genres(headers)

        # Insérer dans la table genres
        for row in all_genres_df.itertuples(index=False):
            cursor.execute("""
                INSERT INTO genres (genre_id, genre_name)
                VALUES (%s, %s)
                ON CONFLICT (genre_id) DO NOTHING;
            """, (row.genre_id, row.genre_name))
        
        print(f"✅ Mise à jour des genres terminée. {len(all_genres_df)} genres insérés ou mis à jour dans la base de données.")
        conn.commit()

    if moviesUpdate:
        # Etape 1 : Mise à jour des films existants dans la base PostgreSQL

        # Étape 1.1 : Récupérer les movie_ids depuis la base PostgreSQL
        cursor.execute(movie_query)
        bdd_movie_ids = [row[0] for row in cursor.fetchall()]   
        print(f"Nombre de films dans la base de données à mettre à jour : {len(bdd_movie_ids)}")

        # Étape 1.2 : Recherche les infos à mettre à jour dans l'API
        updated_movies_df = []
        movie_count = 0

        for i, movie_id in enumerate(bdd_movie_ids, start=1):
            updated_movie = api.update_movie_details(movie_id, headers)
            if updated_movie:
                updated_movies_df.append(updated_movie)
            time.sleep(0.5)  # Respecter les limites de l'API

            movie_count += 1
            print(f"Mise à jour terminée pour le film ID : {movie_id}")
            print(f"Progression : {movie_count}/{len(bdd_movie_ids)} films.")

            # Sauvegarde partielle pour éviter la perte de données en cas de rupture de connexion
            if movie_count % 100 == 0:
                db.insert_updated_movies_to_db(updated_movies_df, conn); updated_movies_df.clear()
            
        # Étape 4 : Insertion des données collectées dans la base de données
        db.insert_updated_movies_to_db(updated_movies_df, conn)
        print(f"✅ Mise à jour des films terminée. {len(updated_movies_df)} films insérés ou mis à jour dans la base de données.")
        conn.commit()

    if peopleUpdate:
        # Étape 2 : Mise à jour de la table people

        # Etape 2.1 : Récupérer les person_ids depuis la base PostgreSQL
        cursor.execute(people_query)
        bdd_person_ids = [row[0] for row in cursor.fetchall()]   
        print(f"Nombre de personnes dans la base de données à mettre à jour : {len(bdd_person_ids)}")

        # Étape 2.2 : Recherche les infos à mettre à jour dans l'API
        updated_people_df = []
        people_count = 0

        for i, person_id in enumerate(bdd_person_ids, start=1):
            updated_people = api.update_person_details(person_id, headers)
            if updated_people:
                updated_people_df.append(updated_people)
            time.sleep(0.5)  # Respecter les limites de l'API

            people_count += 1
            print(f"Mise à jour terminée pour la personne ID : {person_id}")
            print(f"Progression : {people_count}/{len(bdd_person_ids)} personnes.")

            # Sauvegarde partielle pour éviter la perte de données en cas de rupture de connexion
            if people_count % 100 == 0:
                db.insert_updated_people_to_db(updated_people_df, conn); updated_people_df.clear()

        # Étape 5 : Insertion des données collectées dans la base de données
        db.insert_updated_people_to_db(updated_people_df, conn)
        print("\n✅ Mise à jour des personnes terminée. {} personnes insérées ou mises à jour dans la base de données.".format(len(updated_people_df)))
        conn.commit()

    end_time = time.time()

    execution_time = (end_time - start_time)/60
    print(f"🕦 Temps d'exécution : {execution_time:.2f} minutes")

    cursor.close()
    conn.close()
    engine.dispose()