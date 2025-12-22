import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.model_selection import RandomizedSearchCV

# Connexion PostgreSQL
engine = create_engine(
    "postgresql://cynthia:datascientest@34.255.206.116:5432/movie_db"
)

# Requête SQL
SQL = """
SELECT
    m.movie_id,
    m.original_language,
    m.budget,
    m.runtime,
    m.release_date,
    m.revenue,
    mg.genre_id,
    mk.keyword_id,
    mp.person_id,
    mprod.production_id
FROM movies m
LEFT JOIN movie_genres mg      ON m.movie_id = mg.movie_id
LEFT JOIN movie_keywords mk    ON m.movie_id = mk.movie_id
LEFT JOIN movie_people mp      ON m.movie_id = mp.movie_id
LEFT JOIN movie_productions mprod ON m.movie_id = mprod.movie_id
WHERE m.budget > 50000 AND m.revenue > 50000;
"""
df = pd.read_sql(SQL, engine)

# On garde les Top-100 modalités des tables intermédiaires
K = 100
top_genres      = df["genre_id"].value_counts().head(K).index
top_keywords    = df["keyword_id"].value_counts().head(K).index
top_people      = df["person_id"].value_counts().head(K).index
top_productions = df["production_id"].value_counts().head(K).index

# Fonction pour pivot des colonnes binaires 
def make_topK_onehot(df, column, top_values):
    tmp = df[["movie_id", column]].dropna()
    tmp = tmp[tmp[column].isin(top_values)]
    tmp["value"] = 1
    pivot = tmp.pivot_table(index="movie_id",
                            columns=column,
                            values="value",
                            fill_value=0)
    pivot.columns = [f"{column}_{c}" for c in pivot.columns]
    return pivot

genres_ohe      = make_topK_onehot(df, "genre_id",      top_genres)
keywords_ohe    = make_topK_onehot(df, "keyword_id",    top_keywords)
people_ohe      = make_topK_onehot(df, "person_id",     top_people)
prod_ohe        = make_topK_onehot(df, "production_id", top_productions)

# Tableau principal (nettoyage pour avoir qu'une seule ligne par film)
df_base = df.groupby("movie_id").agg({
    "budget": "first",
    "runtime": "first",
    "release_date": "first",
    "revenue": "first",
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
y = X["revenue"]
X = X.drop(columns=["revenue"])

# Séparation des jeux de données et remplacement des NaNs:
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

X_train = X_train.fillna(0)
X_test = X_test.fillna(0)

# Linear Regression
lr = LinearRegression()
lr.fit(X_train, y_train)
print("LinearRegression :")
print("train :", lr.score(X_train, y_train))
print("test  :", lr.score(X_test, y_test))

# RandomForest avec RandomizedSearchCV
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

# XGBRegressor avec RandomizedSearchCV
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

# Top 50 features importantes pour RandomForest si on veut optimiser
feat_imp = pd.DataFrame({
    "feature": X_train.columns,
    "importance": best_rf.feature_importances_
})
feat_imp["importance_pct"] = 100 * feat_imp["importance"] / feat_imp["importance"].sum()
top50 = feat_imp.sort_values("importance_pct", ascending=False).head(50)

print("\nTop 50 features RandomForest :")
print(top50[["feature","importance_pct"]])
