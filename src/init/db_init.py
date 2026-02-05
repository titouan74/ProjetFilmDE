import os
import sys
import db_insertion_postgres as db_ins
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import connect_to_db

# Connexion à la base de données PostgreSQL
engine = connect_to_db()
conn = engine.raw_connection()

# Récupération des données depuis les fichiers CSV
keywords_df = pd.read_csv("data/keywords_data.csv")
genres_df = pd.read_csv("data/genres_data.csv")
productions_df = pd.read_csv("data/productions_data.csv")
people_df = pd.read_csv("data/people_data.csv")
roles_df = pd.read_csv("data/roles_data.csv")
movies_df = pd.read_csv("data/movies_data.csv")
movie_people_df = pd.read_csv("data/movie_people.csv")
movie_productions_df = pd.read_csv("data/movie_productions.csv")
movie_genres_df = pd.read_csv("data/movie_genres.csv")
movie_keywords_df = pd.read_csv("data/movie_keywords.csv")

# Insertion des données dans les tables PostgreSQL
#db_ins.insert_keywords_to_db(conn, keywords_df)
#print("✅ Table keywords successfully filled.")
#db_ins.insert_genres_to_db(conn, genres_df)
#print("✅ Table genres successfully filled.")
#db_ins.insert_productions_to_db(conn, productions_df)
#print("✅ Table productions successfully filled.")
#db_ins.insert_people_to_db(conn, people_df)
#print("✅ Table people successfully filled.")
#db_ins.insert_roles_to_db(conn, roles_df)
#print("✅ Table roles successfully filled.")
#db_ins.insert_movies_to_db(conn, movies_df)
#print("✅ Table movies successfully filled.")
db_ins.insert_movie_people_to_db(conn, movie_people_df)
print("✅ Table movie_people successfully filled.")
db_ins.insert_movie_productions_to_db(conn, movie_productions_df)
print("✅ Table movie_productions successfully filled.")
db_ins.insert_movie_genres_to_db(conn, movie_genres_df)
print("✅ Table movie_genres successfully filled.")
db_ins.insert_movie_keywords_to_db(conn, movie_keywords_df)
print("✅ Table movie_keywords successfully filled.")

# Fermeture de la connexion
conn.commit()
conn.close()
engine.dispose()
