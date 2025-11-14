import psycopg2

# Fonctions d'insertion des données dans la base de données PostgreSQL

def insert_people_to_db(conn, people_df):
    cursor = conn.cursor()
    for person in people_df:
        cursor.execute("""
            INSERT INTO People (person_id, person_name, gender, birthday, deathday, popularity, place_of_birth)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (person_id) DO NOTHING;
        """, (person['person_id'], person['person_name'], person['gender'], person['birthday'], person['deathday'], person['popularity'], person['place_of_birth']))
    conn.commit()

def insert_movie_people_to_db(conn, movie_people_df):
    cursor = conn.cursor()
    for entry in movie_people_df:
        cursor.execute("""
            INSERT INTO Movie_People (movie_id, person_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (movie_id, person_id, role) DO NOTHING;
        """, (entry['movie_id'], entry['person_id'], entry['role']))
    conn.commit()

def insert_movies_to_db(conn, movies_df):
    cursor = conn.cursor()
    for movie in movies_df:
        cursor.execute("""
            INSERT INTO Movies (movie_id, imdb_id, budget, original_title, overview, original_language, production_country, release_date, runtime, vote_average, vote_count, popularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (movie_id) DO NOTHING;
        """, (movie['movie_id'], movie['imdb_id'], movie['budget'], movie['original_title'], movie['overview'], movie['original_language'], movie['production_country'], movie['release_date'], movie['runtime'], movie['vote_average'], movie['vote_count'], movie['popularity']))
    conn.commit()

def insert_movie_genres_to_db(conn, movie_genres_df):
    cursor = conn.cursor()
    for entry in movie_genres_df:
        cursor.execute("""
            INSERT INTO Movie_Genres (movie_id, genre_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, genre_id) DO NOTHING;
        """, (entry['movie_id'], entry['genre_id']))
    conn.commit()

def insert_genres_to_db(conn, genres_df):
    cursor = conn.cursor()
    for genre in genres_df:
        cursor.execute("""
            INSERT INTO Genres (genre_id, genre_name)
            VALUES (%s, %s)
            ON CONFLICT (genre_id) DO NOTHING;
        """, (genre['genre_id'], genre['genre_name']))
    conn.commit()

def insert_productions_to_db(conn, productions_df):
    cursor = conn.cursor()
    for production in productions_df:
        cursor.execute("""
            INSERT INTO Productions (production_id, production_name, origin_country)
            VALUES (%s, %s, %s)
            ON CONFLICT (production_id) DO NOTHING;
        """, (production['production_id'], production['production_name'], production['origin_country']))
    conn.commit()

def insert_movie_productions_to_db(conn, movie_productions_df):
    cursor = conn.cursor()
    for entry in movie_productions_df:
        cursor.execute("""
            INSERT INTO Movie_Productions (movie_id, production_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, production_id) DO NOTHING;
        """, (entry['movie_id'], entry['production_id']))
    conn.commit()

def insert_movie_keywords_to_db(conn, movie_keywords_df):
    cursor = conn.cursor()
    for entry in movie_keywords_df:
        cursor.execute("""
            INSERT INTO Movie_Keywords (movie_id, keyword_id)
            VALUES (%s, %s)
            ON CONFLICT (movie_id, keyword_id) DO NOTHING;
        """, (entry['movie_id'], entry['keyword_id']))
    conn.commit()

def insert_keywords_to_db(conn, keywords_df):
    cursor = conn.cursor()
    for keyword in keywords_df:
        cursor.execute("""
            INSERT INTO Keywords (keyword_id, keyword_name)
            VALUES (%s, %s)
            ON CONFLICT (keyword_id) DO NOTHING;
        """, (keyword['keyword_id'], keyword['keyword_name']))
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

