import psycopg2
import pandas as pd

# Fonctions d'insertion des données dans la base de données PostgreSQL

def insert_people_to_db(conn, people_df):

    # conversion en dataframe si nécessaire
    if isinstance(people_df, list):
        if len(people_df) == 0:
            return  # rien à insérer
        people_df = pd.DataFrame(people_df)
    elif isinstance(people_df, dict):
        people_df = pd.DataFrame([people_df])
    elif not isinstance(people_df, pd.DataFrame):
        raise ValueError("people_df must be list, dict or DataFrame")

    # conversion des dates
    people_df["birthday"] = pd.to_datetime(people_df["birthday"], errors="coerce")
    people_df["deathday"] = pd.to_datetime(people_df["deathday"], errors="coerce")

    cursor = conn.cursor()

    for row in people_df.itertuples(index=False):
        birthday = row.birthday if pd.notnull(row.birthday) else None
        deathday = row.deathday if pd.notnull(row.deathday) else None

        cursor.execute("""
            INSERT INTO People (person_id, person_name, gender, popularity, birthday, deathday)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (person_id) DO NOTHING;
        """,
        (row.person_id, row.person_name, row.gender, row.popularity, birthday, deathday))
    conn.commit()

def insert_movie_people_to_db(conn, movie_people_df):
    # conversion en dataframe si nécessaire
    if isinstance(movie_people_df, list):
        if len(movie_people_df) == 0:
            return  # rien à insérer
        movie_people_df = pd.DataFrame(movie_people_df)
    elif isinstance(movie_people_df, dict):
        movie_people_df = pd.DataFrame([movie_people_df])
    elif not isinstance(movie_people_df, pd.DataFrame):
        raise ValueError("movies_df must be list, dict or DataFrame")

    cursor = conn.cursor()

    for row in movie_people_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Movie_People (movie_id, person_id, role_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (movie_id, person_id, role_id) DO NOTHING;
        """, (row.movie_id, row.person_id, row.role_id))
    conn.commit()

def insert_movies_to_db(conn, movies_df):
    # conversion en dataframe si nécessaire
    if isinstance(movies_df, list):
        if len(movies_df) == 0:
            return  # rien à insérer
        movies_df = pd.DataFrame(movies_df)
    elif isinstance(movies_df, dict):
        movies_df = pd.DataFrame([movies_df])
    elif not isinstance(movies_df, pd.DataFrame):
        raise ValueError("movies_df must be list, dict or DataFrame")

    # conversion des dates
    movies_df["release_date"] = pd.to_datetime(movies_df["release_date"], errors="coerce")

    cursor = conn.cursor()
    
    for row in movies_df.itertuples(index=False):
        release_date = row.release_date if pd.notnull(row.release_date) else None

        cursor.execute("""
            INSERT INTO Movies (movie_id, imdb_id, budget, title, overview, original_language, revenue, status, release_date, runtime, vote_average, vote_count, popularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (movie_id) DO NOTHING;
        """, (row.movie_id, row.imdb_id, row.budget, row.title, row.overview, row.original_language, row.revenue, row.status, release_date, row.runtime, row.vote_average, row.vote_count, row.popularity))
    conn.commit()

def insert_movie_genres_to_db(conn, movie_genres_df):

    # conversion en dataframe si nécessaire
    if isinstance(movie_genres_df, list):
        if len(movie_genres_df) == 0:
            return  # rien à insérer
        movie_genres_df = pd.DataFrame(movie_genres_df)
    elif isinstance(movie_genres_df, dict):
        movie_genres_df = pd.DataFrame([movie_genres_df])
    elif not isinstance(movie_genres_df, pd.DataFrame):
        raise ValueError("movie_genres_df must be list, dict or DataFrame")

    cursor = conn.cursor()

    for row in movie_genres_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Movie_Genres (movie_id, genre_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, genre_id) DO NOTHING;
        """, (row.movie_id, row.genre_id))
    conn.commit()

def insert_genres_to_db(conn, genres_df):
    # conversion en dataframe si nécessaire
    if isinstance(genres_df, list):
        if len(genres_df) == 0:
            return  # rien à insérer
        genres_df = pd.DataFrame(genres_df)
    elif isinstance(genres_df, dict):
        genres_df = pd.DataFrame([genres_df])
    elif not isinstance(genres_df, pd.DataFrame):
        raise ValueError("genres_df must be list, dict or DataFrame")

    cursor = conn.cursor()

    for row in genres_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Genres (genre_id, genre_name)
            VALUES (%s, %s)
            ON CONFLICT (genre_id) DO NOTHING;
        """, (row.genre_id, row.genre_name))
    conn.commit()

def insert_productions_to_db(conn, productions_df):
    # conversion en dataframe si nécessaire
    if isinstance(productions_df, list):
        if len(productions_df) == 0:
            return  # rien à insérer
        productions_df = pd.DataFrame(productions_df)
    elif isinstance(productions_df, dict):
        productions_df = pd.DataFrame([productions_df])
    elif not isinstance(productions_df, pd.DataFrame):
        raise ValueError("productions_df must be list, dict or DataFrame")

    cursor = conn.cursor()

    for row in productions_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Productions (production_id, production_name, origin_country)
            VALUES (%s, %s, %s)
            ON CONFLICT (production_id) DO NOTHING;
        """, (row.production_id, row.production_name, row.origin_country))
    conn.commit()

def insert_movie_productions_to_db(conn, movie_productions_df):
    # conversion en dataframe si nécessaire
    if isinstance(movie_productions_df, list):
        if len(movie_productions_df) == 0:
            return  # rien à insérer
        movie_productions_df = pd.DataFrame(movie_productions_df)
    elif isinstance(movie_productions_df, dict):
        movie_productions_df = pd.DataFrame([movie_productions_df])
    elif not isinstance(movie_productions_df, pd.DataFrame):
        raise ValueError("movie_productions_df must be list, dict or DataFrame")

    cursor = conn.cursor()

    for row in movie_productions_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Movie_Productions (movie_id, production_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, production_id) DO NOTHING;
        """, (row.movie_id, row.production_id))
    conn.commit()

def insert_movie_keywords_to_db(conn, movie_keywords_df):
    # conversion en dataframe si nécessaire
    if isinstance(movie_keywords_df, list):
        if len(movie_keywords_df) == 0:
            return  # rien à insérer
        movie_keywords_df = pd.DataFrame(movie_keywords_df)
    elif isinstance(movie_keywords_df, dict):
        movie_keywords_df = pd.DataFrame([movie_keywords_df])
    elif not isinstance(movie_keywords_df, pd.DataFrame):
        raise ValueError("movie_keywords_df must be list, dict or DataFrame")

    cursor = conn.cursor()

    for row in movie_keywords_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Movie_Keywords (movie_id, keyword_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, keyword_id) DO NOTHING;
        """, (row.movie_id, row.keyword_id))
    conn.commit()

def insert_keywords_to_db(conn, keywords_df):
    # conversion en dataframe si nécessaire
    if isinstance(keywords_df, list):
        if len(keywords_df) == 0:
            return  # rien à insérer
        keywords_df = pd.DataFrame(keywords_df)
    elif isinstance(keywords_df, dict):
        keywords_df = pd.DataFrame([keywords_df])
    elif not isinstance(keywords_df, pd.DataFrame):
        raise ValueError("keywords_df must be list, dict or DataFrame")

    cursor = conn.cursor()

    for row in keywords_df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO Keywords (keyword_id, keyword_name)
            VALUES (%s, %s)
            ON CONFLICT (keyword_id) DO NOTHING;
        """, (row.keyword_id, row.keyword_name))
    conn.commit()

def insert_roles_to_db(conn, roles_df):
    # conversion en dataframe si nécessaire
    if isinstance(roles_df, list):
        if len(roles_df) == 0:
            return  # rien à insérer
        roles_df = pd.DataFrame(roles_df)
    elif isinstance(roles_df, dict):
        roles_df = pd.DataFrame([roles_df])
    elif not isinstance(roles_df, pd.DataFrame):
        raise ValueError("roles_df must be list, dict or DataFrame")

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

def insert_updated_movies_to_db(movies_df, conn):
    # conversion en dataframe si nécessaire
    if not isinstance(movies_df, pd.DataFrame):
        movies_df = pd.DataFrame(movies_df)

    cursor = conn.cursor()
    
    # conversion des dates
    movies_df["release_date"] = pd.to_datetime(movies_df["release_date"], errors="coerce")
    for row in movies_df.itertuples(index=False):
        release_date = row.release_date if pd.notnull(row.release_date) else None

    try:
        cursor.execute("""
            UPDATE Movies
            SET release_date = %s, vote_average = %s, vote_count = %s, popularity = %s, revenue = %s, status = %s
            WHERE movie_id = %s;
        """, (release_date, row.vote_average, row.vote_count, row.popularity, row.revenue, row.status, row.movie_id))

    except Exception as e:
        print("\n--- ERROR ON ROW ---")
        print("movie_id:", row.movie_id)
        print("vote_average:", row.vote_average)
        print("vote_count:", row.vote_count)
        print("popularity:", row.popularity)
        print("revenue:", row.revenue)
        print("status:", row.status)
        print("--------------------\n")
        raise e
    
    conn.commit()

def insert_updated_people_to_db(people_df, conn):
    # conversion en dataframe si nécessaire
    if not isinstance(people_df, pd.DataFrame):
        people_df = pd.DataFrame(people_df)

    cursor = conn.cursor()

    for row in people_df.itertuples(index=False):
        popularity = row.popularity if pd.notnull(row.popularity) else None

        cursor.execute("""
            UPDATE People
            SET popularity = %s
            WHERE person_id = %s;
        """, (popularity, row.person_id))
    conn.commit()