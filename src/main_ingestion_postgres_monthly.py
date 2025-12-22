import requests
import pandas as pd
import credentials
import time
import api_data_ingestion as api
import os
import psycopg2
import db_insertion_postgres as db
from datetime import datetime, timedelta

if __name__ == "__main__":
    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
        dbname="movie_db",
        user="cynthia",
        password="datascientest",
        host="34.244.236.174",
        port="5432"
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
    print(f"⏳ Recherche des nouveaux films depuis l'API...")
    
    # Configuration pour itérer mois par mois sur l'année 2025
    year = 2024
    all_api_movie_ids = []
    
    for month in range(1, 13):
        # Calculer les dates de début et fin pour chaque mois
        start_date = f"{year}-{month:02d}-01"
        
        # Calculer le dernier jour du mois
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        end_date = (next_month - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"Récupération des films pour la période : {start_date} à {end_date}")
        
        # Récupérer les movie_ids pour ce mois
        api_movie_ids = api.get_movie_ids(start_date, end_date, headers)
        all_api_movie_ids.extend(api_movie_ids)
        
        print(f"Films trouvés pour {start_date} à {end_date}: {len(api_movie_ids)}")
        
        # Pause pour respecter les limites de l'API
        time.sleep(0.5)
    
    # Supprimer les doublons
    all_api_movie_ids = list(set(all_api_movie_ids))
    print(f"Total de films uniques trouvés pour l'année {year}: {len(all_api_movie_ids)}")

    # Étape 3 : Identifier les movie_ids qui ne sont pas encore dans la base postgreSQL
    new_movie_ids = list(set(all_api_movie_ids) - set(bdd_movie_ids))
    print(f"Nombre de nouveaux films à ingérer : {len(new_movie_ids)}")

    # Étape 4 : Ingestion des données pour chaque nouveau movie_id
    new_movies_df = []
    new_people_df = []
    new_movie_people_df = []
    new_productions_df = []
    new_movie_productions_df = []
    new_movie_genres_df = []
    new_movie_keywords_df = []
    new_keywords_df = []

    count = 0
    new_count = 0

    for movie_id in new_movie_ids:
        print(f"\nIngestion des données pour le film ID : {movie_id}")

        # Récupérer et stocker les détails du film
        movie_details = api.get_movie_details(movie_id, headers)
        if movie_details and movie_details.get("budget", 0) > 50000:
            new_movies_df.append(movie_details)
            print(f"Le film {movie_id} a bien été récupéré.")

            # Récupérer et stocker les acteurs du film puis récupérer la liste des nouveaux acteurs
            movie_people = api.get_movie_people(movie_id, headers)
            if movie_people:
                new_movie_people_df.extend(movie_people)
                for person in movie_people:
                    # Vérifie si déjà ajouté pendant cette ingestion
                    if any(a['person_id'] == person['person_id'] for a in new_people_df):
                        continue  

                    # Vérifie dans la base PostgreSQL
                    elif db.person_exists_in_db(person['person_id'], conn):
                        continue  

                    # Ajoute le nouvel acteur
                    else:
                        person_details = api.get_people_details(person['person_id'], headers)
                        if person_details:
                            new_people_df.append(person_details)

                    time.sleep(0.25)  # Respecter les limites de l'API
            else:
                print(f"Aucune personne trouvée pour le film ID : {movie_id}")

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
                    # Vérifie si déjà ajouté pendant cette ingestion
                    if any(p['production_id'] == production['production_id'] for p in new_productions_df):
                        continue

                    # Vérifie dans la base PostgreSQL
                    elif db.production_exists_in_db(production['production_id'], conn):
                        continue  # Production déjà présente dans la BDD

                    # Ajoute la nouvelle production
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
                    # Vérifie si déjà ajouté pendant cette ingestion
                    if any(k['keyword_id'] == keyword['keyword_id'] for k in new_keywords_df):
                        continue

                    # Vérifie dans la base PostgreSQL
                    elif db.keyword_exists_in_db(keyword['keyword_id'], conn):
                        continue  # Keyword déjà présent dans la BDD

                    # Ajoute le nouveau keyword
                    else:
                        keyword_details = api.get_keywords_details(keyword['keyword_id'], headers)
                        if keyword_details:
                            new_keywords_df.append(keyword_details)
                    time.sleep(0.25)  # Respecter les limites de l'API

            print(f"Ingestion terminée pour le film ID : {movie_id}")

            count += 1
            new_count += 1
            print(f"Progression : {count}/{len(new_movie_ids)} films.")

        elif movie_details.get("budget", 0) <= 50000:
            print(f"Le film {movie_id} a un budget inférieur ou égal à 50000. Ignoré.")
            count += 1
            print(f"Progression : {count}/{len(new_movie_ids)} films.")
            continue
        else:
            print(f"Échec de la récupération des informations pour le film ID : {movie_id}")
            count += 1
            print(f"Progression : {count}/{len(new_movie_ids)} films.")
            continue

        # Sauvegarde partielle pour éviter la perte de données en cas de rupture de connexion
        if count % 10 == 0:
            db.insert_movies_to_db(conn, new_movies_df); new_movies_df.clear()
            db.insert_people_to_db(conn, new_people_df); new_people_df.clear()
            db.insert_productions_to_db(conn, new_productions_df); new_productions_df.clear()
            db.insert_keywords_to_db(conn, new_keywords_df); new_keywords_df.clear()
            db.insert_movie_people_to_db(conn, new_movie_people_df); new_movie_people_df.clear()
            db.insert_movie_productions_to_db(conn, new_movie_productions_df); new_movie_productions_df.clear()
            db.insert_movie_genres_to_db(conn, new_movie_genres_df); new_movie_genres_df.clear()
            db.insert_movie_keywords_to_db(conn, new_movie_keywords_df); new_movie_keywords_df.clear()
            
            print("💾 Sauvegarde partielle effectuée dans la base de données.")

    # Étape 5 : Insertion des données collectées dans la base de données
    db.insert_movies_to_db(conn, new_movies_df)
    db.insert_people_to_db(conn, new_people_df)
    db.insert_productions_to_db(conn, new_productions_df)
    db.insert_keywords_to_db(conn, new_keywords_df)
    db.insert_movie_people_to_db(conn, new_movie_people_df)
    db.insert_movie_productions_to_db(conn, new_movie_productions_df)
    db.insert_movie_genres_to_db(conn, new_movie_genres_df)
    db.insert_movie_keywords_to_db(conn, new_movie_keywords_df)

    print(f"\n✅ Ingestion des données terminée : {new_count} nouveaux films ajoutés avec succès dans la base de données !")

    end_time = time.time()

    execution_time = (end_time - start_time)/60
    print(f"🕦 Temps d'exécution : {execution_time:.2f} minutes")