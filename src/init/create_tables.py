import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import connect_to_db

# Connexion à la base de données PostgreSQL
engine = connect_to_db()
conn = engine.raw_connection()
cursor = conn.cursor()

schema = """

-- table de faits:

CREATE TABLE IF NOT EXISTS movies (
    movie_id INTEGER PRIMARY KEY,
    title TEXT,
    overview TEXT,
    original_language TEXT,
    popularity NUMERIC,
    budget NUMERIC,
    imdb_id TEXT,
    revenue NUMERIC,
    runtime NUMERIC,
    status TEXT,
    vote_average NUMERIC,
    vote_count INTEGER,
    release_date DATE
);

-- tables de dimensions: 

CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY,
    genre_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS productions (
    production_id INTEGER PRIMARY KEY,
    production_name TEXT,
    origin_country TEXT
);

CREATE TABLE IF NOT EXISTS people (
    person_id INTEGER PRIMARY KEY,
    gender INTEGER,
    person_name TEXT,
    popularity NUMERIC,
    birthday DATE,
    deathday DATE,
    place_of_birth TEXT
);

CREATE TABLE IF NOT EXISTS keywords (
    keyword_id INTEGER PRIMARY KEY,
    keyword_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS roles (
    role_id INTEGER PRIMARY KEY,
    role_name TEXT UNIQUE
);

-- Tables intermédiaires:

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE IF NOT EXISTS movie_productions (
    movie_id INTEGER,
    production_id INTEGER,
    PRIMARY KEY (movie_id, production_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (production_id) REFERENCES productions(production_id)
);

CREATE TABLE IF NOT EXISTS movie_people (
    movie_id INTEGER,
    person_id INTEGER,
    role_id INTEGER,
    PRIMARY KEY (movie_id, person_id, role_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (person_id) REFERENCES people(person_id),
    FOREIGN KEY (role_id) REFERENCES roles(role_id)
);

CREATE TABLE IF NOT EXISTS movie_keywords (
    movie_id INTEGER,
    keyword_id INTEGER,
    PRIMARY KEY (movie_id, keyword_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id)
);
"""

cursor.execute(schema)

cursor.execute("""
    INSERT INTO roles (role_id, role_name)
    VALUES
        (1, 'Writer'),
        (2, 'Director'),
        (3, 'Actor'),
        (4, 'Producer')
    ON CONFLICT (role_id) DO NOTHING;
""")

conn.commit()
cursor.close()
conn.close()
engine.dispose()

print("All tables created successfully.")
