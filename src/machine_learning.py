import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import psycopg2

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname="movie_db",
    user="cynthia",
    password="datascientest",
    host="108.130.31.47",
    port="5432"
)
cursor = conn.cursor()

def fetch_movie_data():
    query = """
    SELECT m.movie_id, m.title, m.release_date, mg.genre_name, k.keyword, p.person_id, p.name, p.role
    FROM movies m
    WHERE m.budget > 50000
    AND m.status = 'Released'
    LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
    LEFT JOIN movie_keywords mk ON m.movie_id = mk.movie_id
    LEFT JOIN keywords k ON mk.keyword_id = k.keyword_id
    LEFT JOIN movie_people mp ON m.movie_id = mp.movie_id
    LEFT JOIN people p ON mp.person_id = p.person_id
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)

def preprocess_data(df):
    df = df.dropna(subset=['genre_name', 'keyword', 'role'])
    df['release_year'] = pd.to_datetime(df['release_date']).dt.year
    df = pd.get_dummies(df, columns=['genre_name', 'keyword', 'role'], drop_first=True)
    return df

def train_model(df):
    X = df.drop(columns=['movie_id', 'title', 'release_date', 'person_id', 'name'])
    y = df['genre_name_Action']  # Example: Predicting if genre is Action

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Model Accuracy: {accuracy:.2f}")

    return model