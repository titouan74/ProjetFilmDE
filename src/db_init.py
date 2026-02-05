import psycopg2
import db_insertion_postgres as db_ins
import pandas as pd

# Connexion à la base PostgreSQL
conn = psycopg2.connect(
    dbname="movie_db",
    user="titouan",
    password="datascientest",
    host="108.130.172.218",
    port=5432
)

# -----------------------------
# 1️⃣ Lecture des CSV avec dtype int pour les IDs
# -----------------------------
keywords_df = pd.read_csv("data/keywords_data.csv", dtype={'keyword_id': int})
genres_df = pd.read_csv("data/genres_data.csv", dtype={'genre_id': int})
productions_df = pd.read_csv("data/productions_data.csv", dtype={'production_id': int})
people_df = pd.read_csv("data/people_data.csv", dtype={'person_id': int})
roles_df = pd.read_csv("data/roles_data.csv", dtype={'role_id': int})
movies_df = pd.read_csv("data/movies_data.csv", dtype={'movie_id': int})
movie_people_df = pd.read_csv("data/movie_people.csv", dtype={'movie_id': int, 'person_id': int, 'role_id': int})
movie_productions_df = pd.read_csv("data/movie_productions.csv", dtype={'movie_id': int, 'production_id': int})
movie_genres_df = pd.read_csv("data/movie_genres.csv", dtype={'movie_id': int, 'genre_id': int})
movie_keywords_df = pd.read_csv("data/movie_keywords.csv", dtype={'movie_id': int, 'keyword_id': int})

# -----------------------------
# 2️⃣ Filtrage des tables intermédiaires pour respecter les FK
# -----------------------------
movie_people_df = movie_people_df[
    movie_people_df['movie_id'].isin(movies_df['movie_id']) &
    movie_people_df['person_id'].isin(people_df['person_id']) &
    movie_people_df['role_id'].isin(roles_df['role_id'])
]

movie_productions_df = movie_productions_df[
    movie_productions_df['movie_id'].isin(movies_df['movie_id']) &
    movie_productions_df['production_id'].isin(productions_df['production_id'])
]

movie_genres_df = movie_genres_df[
    movie_genres_df['movie_id'].isin(movies_df['movie_id']) &
    movie_genres_df['genre_id'].isin(genres_df['genre_id'])
]

movie_keywords_df = movie_keywords_df[
    movie_keywords_df['movie_id'].isin(movies_df['movie_id']) &
    movie_keywords_df['keyword_id'].isin(keywords_df['keyword_id'])
]

# -----------------------------
# 3️⃣ Insertion dans les tables parents
# -----------------------------
#db_ins.insert_roles_to_db(conn, roles_df)
#print("✅ Table roles filled.")

#db_ins.insert_movies_to_db(conn, movies_df)
#print("✅ Table movies filled.")

#db_ins.insert_people_to_db(conn, people_df)
#print("✅ Table people filled.")

#db_ins.insert_genres_to_db(conn, genres_df)
#print("✅ Table genres filled.")

#db_ins.insert_productions_to_db(conn, productions_df)
#print("✅ Table productions filled.")

#db_ins.insert_keywords_to_db(conn, keywords_df)
#print("✅ Table keywords filled.")

# -----------------------------
# 4️⃣ Insertion dans les tables intermédiaires
# -----------------------------
db_ins.insert_movie_people_to_db(conn, movie_people_df)
print("✅ Table movie_people filled.")

db_ins.insert_movie_productions_to_db(conn, movie_productions_df)
print("✅ Table movie_productions filled.")

db_ins.insert_movie_genres_to_db(conn, movie_genres_df)
print("✅ Table movie_genres filled.")

db_ins.insert_movie_keywords_to_db(conn, movie_keywords_df)
print("✅ Table movie_keywords filled.")

# -----------------------------
# 5️⃣ Commit et fermeture
# -----------------------------
conn.commit()
conn.close()
print("✅ Toutes les tables ont été remplies avec succès.")
