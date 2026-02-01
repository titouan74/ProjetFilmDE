# =============================
# 1. Imports et configurations
# =============================
import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from xgboost import XGBRegressor
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import time
from joblib import dump, load
import os
from datetime import datetime


# Configuration pour les graphiques
plt.style.use('default')
sns.set_palette("husl")

# =============================
# 2. Paramètres de connexion et de la requête SQL
# =============================

# Connexion SQLAlchemy (recommandée pour pandas)
engine = create_engine('postgresql://cynthia:datascientest@108.129.220.163:5432/movie_db', 
                       connect_args={"connect_timeout": 10})

# =============================
# 3. Définition des fonctions de traitement des données
# =============================

# Génération de la requête SQL à partir de la target
def build_query(target: str) -> str:
    allowed_targets = ['vote_average', 'revenue', 'popularity']

    if target not in allowed_targets:
        raise ValueError(f"Target '{target}' non autorisée. Choisir parmi {allowed_targets}.")

    return f"""
    SELECT 
        m.movie_id, 
        m.title, 
        m.release_date, 
        m.runtime,
        m.original_language,
        m.budget,
        m.{target} AS target,
        mg.genre_id, 
        mp.person_id,
        p.popularity AS person_popularity,
        mp.role_id,
        mpd.production_id,
        pd.origin_country as production_country,
        mk.keyword_id
    FROM movies m
    LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
    LEFT JOIN movie_people mp ON m.movie_id = mp.movie_id
    LEFT JOIN people p ON mp.person_id = p.person_id
    LEFT JOIN movie_productions mpd ON m.movie_id = mpd.movie_id
    LEFT JOIN productions pd ON mpd.production_id = pd.production_id
    LEFT JOIN movie_keywords mk ON m.movie_id = mk.movie_id
    WHERE m.budget > 50000
    AND m.revenue > 50000
    AND m.status = 'Released'
    AND m.vote_count > 100
    AND m.release_date > '2000-01-01';
"""

# Fonction pour charger les données depuis PostgreSQL
def fetch_movie_data(query: str, engine) -> pd.DataFrame:
    """
    Charge les données depuis PostgreSQL via SQLAlchemy.
    """
    try:
        print("🔗 Test de connexion à la base de données...")
        # Test simple de connexion
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            print("✅ Connexion PostgreSQL réussie!")
        
        df = pd.read_sql(text(query), engine)
        print(f"✅ Données chargées avec succès. Shape: {df.shape}")
        return df
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement des données: {e}")
        print(f"🔍 Type d'erreur: {type(e).__name__}")
        return pd.DataFrame()

# Fonction pour pivot des colonnes binaires 
def make_topK_onehot(df: pd.DataFrame, column: str, top_values: List) -> pd.DataFrame:
    tmp = df[["movie_id", column]].dropna()
    tmp = tmp[tmp[column].isin(top_values)]
    tmp["value"] = 1
    pivot = tmp.pivot_table(index="movie_id",
                            columns=column,
                            values="value",
                            fill_value=0)
    pivot.columns = [f"{column}_{c}" for c in pivot.columns]
    return pivot

# Fonction de préprocessing des données
def preprocess_data(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    Préprocessing des données avec création des features temporelles.
    """
    if df.empty:
        print("⚠️ DataFrame vide reçu")
        return None, None

    df = df.copy()

    # On garde les Top-100 modalités des tables intermédiaires
    K = 100
    top_genres      = df["genre_id"].value_counts().head(K).index
    top_keywords    = df["keyword_id"].value_counts().head(K).index
    top_people      = df["person_id"].value_counts().head(K).index
    top_productions = df["production_id"].value_counts().head(K).index

    # OneHotEncoding des tables intermédiaires
    genres_ohe      = make_topK_onehot(df, "genre_id",      top_genres)
    keywords_ohe    = make_topK_onehot(df, "keyword_id",    top_keywords)
    people_ohe      = make_topK_onehot(df, "person_id",     top_people)
    prod_ohe        = make_topK_onehot(df, "production_id", top_productions)

    # Tableau principal (nettoyage pour avoir qu'une seule ligne par film)
    df_base = df.groupby("movie_id").agg({
        "budget": "first",
        "runtime": "first",
        "release_date": "first",
        "target": "first",
        "original_language": "first"
    })

    # Formatisation de la release_date
    df_base["release_date"] = pd.to_datetime(df_base["release_date"], errors="coerce")
    df_base["year"] = df_base["release_date"].dt.year
    df_base["month"] = df_base["release_date"].dt.month
    df_base["month_sin"] = np.sin(2 * np.pi * df_base["month"] / 12)
    df_base["month_cos"] = np.cos(2 * np.pi * df_base["month"] / 12)
    df_base = df_base.drop(columns=["release_date", "month"])

    # Standardisation variables continues
    scaler = StandardScaler()
    cols_to_scale = ["budget", "runtime", "year"]
    df_base[cols_to_scale] = scaler.fit_transform(df_base[cols_to_scale])

    # OneHotEncoder sur la langue
    ohe = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
    lang_encoded = ohe.fit_transform(df_base[["original_language"]])
    lang_columns = [f"lang_{cat}" for cat in ohe.categories_[0]]
    lang_df = pd.DataFrame(lang_encoded, index=df_base.index, columns=lang_columns)

    df_base = df_base.drop(columns=["original_language"])
    df_base = df_base.join(lang_df)

    # Merge final pivot + tableau principal
    X = df_base.join([genres_ohe, keywords_ohe, people_ohe, prod_ohe])
    y = X["target"]
    X = X.drop(columns=["target"])

    X = X.fillna(0)

    return X, y

# =============================
# 4. Définition des fonctions de machine learning
# =============================

# Linear Regression
def process_linear_regression(X: pd.DataFrame, y: pd.Series,
                              test_size: float = 0.2, random_state: int = 42):
    """"
    Entraîne un modèle de régression linéaire sur X/y.
    Retourne le modèle entraîné et les splits train/test.
    """
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    print("LinearRegression :")
    print("train :", lr.score(X_train, y_train))
    print("test  :", lr.score(X_test, y_test))
    return lr, X_train, X_test, y_train, y_test

# RandomForest avec RandomizedSearchCV
def process_random_forest(X: pd.DataFrame, y: pd.Series,
                          featImportance: bool = True,
                          test_size: float = 0.2, random_state: int = 42):
    """
    Entraîne un RandomForestRegressor avec RandomizedSearchCV sur X/y.
    Retourne le meilleur modèle et les splits train/test.
    """
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

    rf = RandomForestRegressor(random_state=42, n_jobs=-1)
    rf_params = {
        "n_estimators": [100, 200, 300],
        "max_depth": [10, 20, 30, None],
        "min_samples_split": [2, 5, 10],
        "min_samples_leaf": [1, 2, 4],
        "max_features": ["sqrt","log2", None]
    }

    rf_search = RandomizedSearchCV(
        rf, rf_params, n_iter=10, cv=3, scoring="r2", n_jobs=-1, random_state=42
    )
    rf_search.fit(X_train, y_train)
    best_rf = rf_search.best_estimator_
    print("RandomForest :")
    print("Meilleurs paramètres RandomForest :", rf_search.best_params_)
    print("train :", best_rf.score(X_train, y_train))
    print("test  :", best_rf.score(X_test, y_test))

    # Top 50 features importantes pour RandomForest si on veut optimiser
    if featImportance:
        feat_imp = pd.DataFrame({
        "feature": X_train.columns,
        "importance": best_rf.feature_importances_
        })
        feat_imp["importance_pct"] = 100 * feat_imp["importance"] / feat_imp["importance"].sum()
        top50 = feat_imp.sort_values("importance_pct", ascending=False).head(50)

        print("\nTop 50 features RandomForest :")
        print(top50[["feature","importance_pct"]])

    return best_rf, X_train, X_test, y_train, y_test

# XGBRegressor avec RandomizedSearchCV
def process_xgb_regressor(X: pd.DataFrame, y: pd.Series,
                          test_size: float = 0.2, random_state: int = 42):
    """
    Entraîne un XGBRegressor avec RandomizedSearchCV sur X/y.
    Retourne le meilleur modèle et les splits train/test.
    """

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

    xgb = XGBRegressor(random_state=42, n_jobs=-1)
    xgb_params = {
        "n_estimators": [100, 200, 300],
        "max_depth": [3, 5, 7],
        "learning_rate": [0.05, 0.1, 0.2],
        "subsample": [0.7, 0.8, 1.0],
        "colsample_bytree": [0.7, 0.8, 1.0]
    }

    xgb_search = RandomizedSearchCV(
        xgb, xgb_params, n_iter=10, cv=3, scoring="r2", n_jobs=-1, random_state=42
    )
    xgb_search.fit(X_train, y_train)
    best_xgb = xgb_search.best_estimator_

    print("XGBRegressor :")
    print("train :", best_xgb.score(X_train, y_train))
    print("test  :", best_xgb.score(X_test, y_test))
    print("Meilleurs paramètres XGBRegressor :", xgb_search.best_params_)

    return best_xgb, X_train, X_test, y_train, y_test

# =============================
# 5. Fonctions de prédiction pour films individuels
# =============================

def predict_movie_target(movie_info: Dict, model, metadata: Dict = None) -> float:
    """
    Prédit la target pour un film donné.
    
    Parameters:
    movie_info: Dict avec les infos du film {'title', 'budget', 'runtime', 'release_date', 
                'original_language', 'genres', 'actors', 'keywords', 'productions'}
    model: Modèle entraîné (LinearRegression, RandomForest ou XGBoost)
    metadata: Métadonnées du modèle (optionnel, chargé automatiquement si non fourni)
    
    Returns:
    float: Prédiction de la target
    """
    
    # Si pas de métadonnées fournies, on utilise les colonnes par défaut
    if metadata and 'feature_columns' in metadata:
        X_columns = metadata['feature_columns']
    else:
        # Colonnes par défaut estimées (vous pouvez ajuster selon votre dataset)
        print("⚠️ Pas de métadonnées, utilisation des colonnes par défaut")
        X_columns = ['budget', 'runtime', 'year', 'month_sin', 'month_cos'] # sera complété automatiquement
    
    # Créer un DataFrame vide avec les colonnes et dtype numérique
    movie_df = pd.DataFrame(columns=X_columns, index=[0], dtype=float)
    movie_df = movie_df.fillna(0).infer_objects(copy=False)  # Initialiser tout à 0
    
    # === VARIABLES DE BASE (normalisées approximativement) ===
    if 'budget' in movie_info and 'budget' in movie_df.columns:
        # Normalisation simple (moyenne ~50M, std ~100M)
        budget_normalized = (movie_info['budget'] - 50000000) / 100000000
        movie_df.loc[0, 'budget'] = float(budget_normalized)
    
    if 'runtime' in movie_info and 'runtime' in movie_df.columns:
        # Normalisation simple (moyenne ~110min, std ~20min)
        runtime_normalized = (movie_info['runtime'] - 110) / 20
        movie_df.loc[0, 'runtime'] = float(runtime_normalized)
    
    # === DATE DE SORTIE ===
    if 'release_date' in movie_info:
        try:
            release_date = pd.to_datetime(movie_info['release_date'])
            year = release_date.year
            month = release_date.month
            
            if 'year' in movie_df.columns:
                # Normalisation année (moyenne ~2010, std ~10)
                year_normalized = (year - 2010) / 10
                movie_df.loc[0, 'year'] = float(year_normalized)
                
            if 'month_sin' in movie_df.columns:
                movie_df.loc[0, 'month_sin'] = float(np.sin(2 * np.pi * month / 12))
            if 'month_cos' in movie_df.columns:
                movie_df.loc[0, 'month_cos'] = float(np.cos(2 * np.pi * month / 12))
        except:
            pass
    
    # === LANGUE ===
    if 'original_language' in movie_info and movie_info['original_language']:
        lang_col = f"lang_{movie_info['original_language']}"
        if lang_col in movie_df.columns:
            movie_df.loc[0, lang_col] = 1
    
    # === GENRES ===
    if 'genres' in movie_info and movie_info['genres']:
        for genre_id in movie_info['genres']:
            genre_col = f"genre_id_{genre_id}"
            if genre_col in movie_df.columns:
                movie_df.loc[0, genre_col] = 1
    
    # === ACTEURS ===
    if 'actors' in movie_info and movie_info['actors']:
        for person_id in movie_info['actors']:
            person_col = f"person_id_{person_id}"
            if person_col in movie_df.columns:
                movie_df.loc[0, person_col] = 1
    
    # === MOTS-CLÉS ===
    if 'keywords' in movie_info and movie_info['keywords']:
        for keyword_id in movie_info['keywords']:
            keyword_col = f"keyword_id_{keyword_id}"
            if keyword_col in movie_df.columns:
                movie_df.loc[0, keyword_col] = 1
    
    # === PRODUCTIONS ===
    if 'productions' in movie_info and movie_info['productions']:
        for prod_id in movie_info['productions']:
            prod_col = f"production_id_{prod_id}"
            if prod_col in movie_df.columns:
                movie_df.loc[0, prod_col] = 1
    
    # Faire la prédiction
    try:
        prediction = model.predict(movie_df)[0]
        return prediction
    except Exception as e:
        print(f"❌ Erreur lors de la prédiction: {e}")
        return 0.0

def get_movie_info_from_db(movie_title: str, engine) -> Dict:
    """
    Récupère les informations d'un film depuis la base de données.
    """
    query = f"""
    SELECT DISTINCT
        m.movie_id,
        m.title,
        m.budget,
        m.runtime,
        m.release_date,
        m.original_language,
        ARRAY_AGG(DISTINCT mg.genre_id) FILTER (WHERE mg.genre_id IS NOT NULL) as genres,
        ARRAY_AGG(DISTINCT mp.person_id) FILTER (WHERE mp.person_id IS NOT NULL) as actors,
        ARRAY_AGG(DISTINCT mk.keyword_id) FILTER (WHERE mk.keyword_id IS NOT NULL) as keywords,
        ARRAY_AGG(DISTINCT mpd.production_id) FILTER (WHERE mpd.production_id IS NOT NULL) as productions
    FROM movies m
    LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
    LEFT JOIN movie_people mp ON m.movie_id = mp.movie_id
    LEFT JOIN movie_keywords mk ON m.movie_id = mk.movie_id
    LEFT JOIN movie_productions mpd ON m.movie_id = mpd.movie_id
    WHERE m.title ILIKE '%{movie_title}%'
    GROUP BY m.movie_id, m.title, m.budget, m.runtime, m.release_date, m.original_language
    LIMIT 1;
    """
    
    try:
        df = pd.read_sql(text(query), engine)
        if df.empty:
            print(f"❌ Film '{movie_title}' non trouvé")
            return {}
        
        movie_data = df.iloc[0].to_dict()
        print(f"✅ Film trouvé: {movie_data['title']}")
        return movie_data
        
    except Exception as e:
        print(f"❌ Erreur lors de la recherche: {e}")
        return {}
    