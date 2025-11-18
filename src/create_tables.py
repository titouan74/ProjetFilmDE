import psycopg2

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname="movies_db",
    user="movie_user",
    password="your_password",
    host="localhost",
    port="5432"
)
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
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS productions (
    production_id INTEGER PRIMARY KEY,
    name TEXT,
    origin_country TEXT
);

CREATE TABLE IF NOT EXISTS people (
    person_id INTEGER PRIMARY KEY,
    gender INTEGER,
    name TEXT,
    popularity NUMERIC,
    birthday DATE,
    deathday DATE,
    place_of_birth TEXT
);

CREATE TABLE IF NOT EXISTS keywords (
    keyword_id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS actors (
    actor_id INTEGER PRIMARY KEY,
    gender INTEGER,
    name TEXT,
    popularity NUMERIC
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
    people_id INTEGER,
    role TEXT,
    PRIMARY KEY (movie_id, people_id, role),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (people_id) REFERENCES people(person_id)
);

CREATE TABLE IF NOT EXISTS movie_keywords (
    movie_id INTEGER,
    keyword_id INTEGER,
    PRIMARY KEY (movie_id, keyword_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id)
);

CREATE TABLE IF NOT EXISTS movie_actors (
    movie_id INTEGER,
    actor_id INTEGER,
    PRIMARY KEY (movie_id, actor_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
);
"""

cursor.execute(schema)
conn.commit()
cursor.close()
conn.close()

print("All tables created successfully.")
