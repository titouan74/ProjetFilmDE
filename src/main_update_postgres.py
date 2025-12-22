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
        user="cynthia",
        password="datascientest",
        host="108.129.186.67",
        port="5432"
    )
    cursor = conn.cursor()

    # Configuration de l'API
    api_key = credentials.api_key
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    # Paramètres de mise à jour
    moviesParam = False
    peopleParam = True
    movie_query = f"""
        SELECT movie_id 
        FROM movies 
        WHERE m.budget > 50000
            AND m.revenue > 50000
            AND m.vote_count > 100
            AND m.release_date > '2000-01-01'
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
    print("Démarrage de l'ingestion des données depuis l'API TMDB...")

    if moviesParam:
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

    if peopleParam:
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
        db.insert_updated_movies_to_db(updated_movies_df, conn)
        db.insert_updated_people_to_db(updated_people_df, conn)
        print("\n✅ Ingestion des données terminée. Données sauvegardées avec succès dans la base de données !")

    end_time = time.time()

    execution_time = (end_time - start_time)/60
    print(f"🕦 Temps d'exécution : {execution_time:.2f} minutes")