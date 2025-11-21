import psycopg2
import pandas as pd

# Fonctions d'insertion des données dans la base de données PostgreSQL

def insert_people_to_db(conn, people_df):
    cursor = conn.cursor()

    # conversion des dates
    people_df["birthday"] = pd.to_datetime(people_df["birthday"], errors="coerce")
    people_df["deathday"] = pd.to_datetime(people_df["deathday"], errors="coerce")

    for row in people_df.itertuples(index=False):
        birthday = row.birthday if pd.notnull(row.birthday) else None
        deathday = row.deathday if pd.notnull(row.deathday) else None

        cursor.execute("""
            INSERT INTO People (person_id, person_name, gender, birthday, deathday)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (person_id) DO NOTHING;
        """,
        (row.person_id, row.person_name, row.gender, birthday, deathday))

    conn.commit()

def insert_movie_people_to_db(conn, movie_people_df):
    cursor = conn.cursor()
    for row in movie_people_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Movie_People (movie_id, person_id, role_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (movie_id, person_id, role_id) DO NOTHING;
        """, (row.movie_id, row.person_id, row.role_id))
    conn.commit()

def insert_movies_to_db(conn, movies_df):
    cursor = conn.cursor()
    # conversion des dates
    movies_df["release_date"] = pd.to_datetime(movies_df["release_date"], errors="coerce")
    for row in movies_df.itertuples(index=False):
        release_date = row.release_date if pd.notnull(row.release_date) else None

        cursor.execute("""
            INSERT INTO Movies (movie_id, imdb_id, budget, title, overview, original_language, release_date, runtime, vote_average, vote_count, popularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (movie_id) DO NOTHING;
        """, (row.movie_id, row.imdb_id, row.budget, row.title, row.overview, row.original_language, release_date, row.runtime, row.vote_average, row.vote_count, row.popularity))
    conn.commit()

def insert_movie_genres_to_db(conn, movie_genres_df):
    cursor = conn.cursor()
    for row in movie_genres_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Movie_Genres (movie_id, genre_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, genre_id) DO NOTHING;
        """, (row.movie_id, row.genre_id))
    conn.commit()

def insert_genres_to_db(conn, genres_df):
    cursor = conn.cursor()
    for row in genres_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Genres (genre_id, genre_name)
            VALUES (%s, %s)
            ON CONFLICT (genre_id) DO NOTHING;
        """, (row.genre_id, row.genre_name))
    conn.commit()

def insert_productions_to_db(conn, productions_df):
    cursor = conn.cursor()
    for row in productions_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Productions (production_id, production_name, origin_country)
            VALUES (%s, %s, %s)
            ON CONFLICT (production_id) DO NOTHING;
        """, (row.production_id, row.production_name, row.origin_country))
    conn.commit()

def insert_movie_productions_to_db(conn, movie_productions_df):
    cursor = conn.cursor()
    for row in movie_productions_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Movie_Productions (movie_id, production_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, production_id) DO NOTHING;
        """, (row.movie_id, row.production_id))
    conn.commit()

def insert_movie_keywords_to_db(conn, movie_keywords_df):
    cursor = conn.cursor()
    for row in movie_keywords_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Movie_Keywords (movie_id, keyword_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, keyword_id) DO NOTHING;
        """, (row.movie_id, row.keyword_id))
    conn.commit()

def insert_keywords_to_db(conn, keywords_df):
    cursor = conn.cursor()
    for row in keywords_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Keywords (keyword_id, keyword_name)
            VALUES (%s, %s)
            ON CONFLICT (keyword_id) DO NOTHING;
        """, (row.keyword_id, row.keyword_name))
    conn.commit()

def insert_roles_to_db(conn, roles_df):
    cursor = conn.cursor()
    for row in roles_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Roles (role_id, role_name)
            VALUES (%s, %s)
            ON CONFLICT (role_id) DO NOTHING;
        """, (row.role_id, row.role_name))
    conn.commit()

def person_exists_in_db(person_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM People WHERE person_id = %s;", (person_id,))
    return cursor.fetchone() is not None

def production_exists_in_db(production_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM Productions WHERE production_id = %s;", (production_id,))
    return cursor.fetchone() is not None

def genre_exists_in_db(genre_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM Genres WHERE genre_id = %s;", (genre_id,))
    return cursor.fetchone() is not None

def keyword_exists_in_db(keyword_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM Keywords WHERE keyword_id = %s;", (keyword_id,))
    return cursor.fetchone() is not None

def movie_exists_in_db(movie_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM Movies WHERE movie_id = %s;", (movie_id,))
    return cursor.fetchone() is not None

