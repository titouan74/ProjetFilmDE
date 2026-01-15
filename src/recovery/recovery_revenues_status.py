import pandas as pd
import ingestion.credentials as credentials
import time
import ingestion.api_data_ingestion as api
import os
import psycopg2
import init.db_insertion_postgres as db
import requests


if __name__ == "__main__":
    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
        dbname="movie_db",
        user="cynthia",
        password="datascientest",
        host="108.130.31.47",
        port="5432"
    )
    cursor = conn.cursor()

    # Récupérer les ids de films qui ont un budget > 50000 dans la base de données PostgreSQL
    cursor.execute("SELECT movie_id FROM movies WHERE budget > 50000")
    high_budget_movie_ids = [row[0] for row in cursor.fetchall()]
    movie_count = 0
    movie_total = len(high_budget_movie_ids)
    print(f"Nombre de films avec un budget > 50000 : {movie_total}")

    # Configuration de l'API
    api_key = credentials.api_key
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    # Ingestion des données pour chaque movie_id avec un budget > 50000
    recovered_df = []

    for movie_id in high_budget_movie_ids:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
        response = requests.get(url, headers=headers)

        try:
            content = response.json()
            movie_details = {
                "movie_id": movie_id,
                "revenue": content.get("revenue", None),
                "status": content.get("status", None)
            }

            print(f"✅ Les données du film {movie_id} ont bien été récupérées.")
            movie_count += 1
            print(f"Progression : {movie_count}/{movie_total} films.")

        except Exception as e:
            print(f"❌ Erreur lors de la récupération des données du film {movie_id} : {e}")
        
        details = pd.DataFrame([movie_details])
        if not details.empty:
            recovered_df.append(details)
        time.sleep(0.5)  # Respecter les limites de l'API

    # Combiner toutes les données récupérées en un seul DataFrame
    if recovered_df:
        final_recovered_df = pd.concat(recovered_df, ignore_index=True)
        
        print("Insertion des données de revenus et de statut dans la base de données PostgreSQL...")
        # Insertion des données récupérées dans la base de données PostgreSQL
        for row in final_recovered_df.itertuples(index=False):
            cursor.execute(
                """
                UPDATE movies
                SET revenue = %s, status = %s
                WHERE movie_id = %s
                """,
                (row.revenue, row.status, row.movie_id)
            )

        conn.commit()
        print("✅ Les données de revenus et de statut ont été insérées dans la base de données PostgreSQL.")
    else:
        print("Aucune donnée récupérée à insérer dans la base de données.")