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


# Configuration pour les graphiques
plt.style.use('default')
sns.set_palette("husl")

# =============================
# 2. Paramètres de connexion et de la requête SQL
# =============================

# Connexion SQLAlchemy (recommandée pour pandas)
engine = create_engine('postgresql://cynthia:datascientest@34.244.118.34:5432/movie_db', 
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

if __name__ == "__main__":
    
    # Début du timer
    start_time = time.time()

    print("🚀 Démarrage du script de machine learning...")
    
    # Sélection de la variable cible à prédire parmi ['vote_average', 'revenue', 'popularity']
    target = 'revenue'  
    print(f"📊 Variable cible sélectionnée: {target}")

    # Construction de la requête SQL et chargement des données
    print("🔍 Construction de la requête SQL...")
    query = build_query(target=target)
    
    print("📥 Chargement des données complètes...")
    df_raw = fetch_movie_data(query, engine)

    if df_raw.empty:
        print("❌ Aucune donnée récupérée de la base de données!")
        exit()

    # Préprocessing des données et obtention de X et y
    print("🔧 Début du préprocessing des données...")
    X, y = preprocess_data(df_raw)

    # Vérification que les données ont bien été prétraitées
    if X is None or y is None:
        print("❌ Le prétraitement des données a échoué. Fin du programme.")
    else:
        print(f"✅ Préprocessing terminé! Shape final: X{X.shape}, y{y.shape}")
        print("\n" + "="*50)
        print("ENTRAÎNEMENT DES MODÈLES")
        print("="*50)
        
        # Entraînement et évaluation des modèles
        print("\n1️⃣ Linear Regression:")
        process_linear_regression(X, y)
        
        print("\n2️⃣ Random Forest:")
        process_random_forest(X, y)
        
        print("\n3️⃣ XGBoost:")
        process_xgb_regressor(X, y)
        
        print("\n🎉 Tous les modèles ont été entraînés avec succès!")
        
        # Fin du timer
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\n⏱️ Temps total d'exécution: {elapsed_time:.2f} secondes")