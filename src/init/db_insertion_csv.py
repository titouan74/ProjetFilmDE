import pandas as pd
import os

def save_data_to_csv(csv_path, data):
    # 🔹 Toujours convertir en DataFrame d'abord
    if isinstance(data, list):
        data = pd.DataFrame(data)
    elif not isinstance(data, pd.DataFrame):
        raise TypeError("Le paramètre 'data' doit être une liste de dictionnaires ou un DataFrame.")

    # 🔹 Fusion ou création
    if os.path.exists(csv_path):
        df_existing = pd.read_csv(csv_path)
        df_combined = pd.concat([df_existing, data], ignore_index=True)
        df_combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"✅ Données ajoutées au fichier CSV : {csv_path} ({len(data)} nouvelles lignes)")
    else:
        data.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"✅ Fichier CSV créé et données ajoutées : {csv_path} ({len(data)} lignes)")

# Define if an actor exists in the database
def actor_exists_in_db(actor_id, csv_path):
    df = pd.read_csv(csv_path)
    return actor_id in df['actor_id'].values

# Define if a genre exists in the database
def genre_exists_in_db(genre_id, csv_path):
    df = pd.read_csv(csv_path)
    return genre_id in df['id'].values

# Define if a production company exists in the database
def production_exists_in_db(production_id, csv_path):
    df = pd.read_csv(csv_path)
    return production_id in df['production_id'].values

# Define if a person exists in the database
def people_exists_in_db(people_id, people_path):
    try:
        if not os.path.exists(people_path):
            return False
        df = pd.read_csv(people_path)
        if df.empty or 'person_id' not in df.columns:
            return False
        return people_id in df['person_id'].values
    except Exception:
        return False

# Define if a keyword exists in the database
def keyword_exists_in_db(keyword_id, keywords_path):
    try:
        if not os.path.exists(keywords_path):
            return False
        df = pd.read_csv(keywords_path)
        if df.empty or 'keyword_id' not in df.columns:
            return False
        return keyword_id in df['keyword_id'].values
    except Exception:
        return False