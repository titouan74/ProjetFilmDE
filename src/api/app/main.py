from fastapi import FastAPI, Header, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional, List
import uvicorn
import pandas as pd
import base64
import sys
import os
from datetime import date, datetime
import sqlalchemy

# Ajouter le dossier parent (src) au chemin Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# Ajouter le dossier courant (api) au chemin Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import client
import ml.machine_learning_utils as ml_utils
import db_connection as dbc

app = FastAPI(title="Movie Success Prediction API")

class MoviePredictionRequest(BaseModel):
    movie_title: str
    target: str
    model: Optional[str] = "xgb"

class MoviePredictionResponse(BaseModel):
    movie_title: str
    budget: float
    language: str
    release_date: date
    target: str
    prediction: float

class AvailableModelResponse(BaseModel):
    model_list: List[dict]

class ModelPerformanceResponse(BaseModel):
    model: str
    target: str
    rmse: float
    r2: float
    mae: float
    last_trained: Optional[date] = None

class ModelPerformanceRequest(BaseModel):
    model: str
    target: str

class MovieProductionResponse(BaseModel):
    movie_title: str
    popularity: float
    budget: float
    status: str
    release_date: date

@app.get("/health")
async def health_check():
    """Vérification de l'état de l'API"""
    return {"L'API est prête à recevoir des requêtes !"}

@app.get("/status")
async def status():
    try:
        engine = dbc.connect_to_db()
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except:
        return {"status": "error", "db": "disconnected"}

@app.post("/predict", response_model=MoviePredictionResponse)
async def predict_movie_success(request: MoviePredictionRequest):
    """Prédire le succès d'un film basé sur son titre et la variable cible"""
    movie_info = ml_utils.get_movie_info_from_db(request.movie_title, engine=dbc.connect_to_db())
    if not movie_info:
        raise HTTPException(status_code=404, detail="Film non trouvé dans la base de données")
    
    model = client.load_model(request.model, request.target)
    metadata = client.load_metadata(request.model, request.target)
    
    prediction = ml_utils.predict_movie_target(movie_info, model, metadata)
    
    response = MoviePredictionResponse(
        movie_title=movie_info.get('title', 'N/A'),
        budget=movie_info.get('budget', 0),
        language=movie_info.get('original_language', 'N/A'),
        release_date=movie_info.get('release_date', date(1900, 1, 1)),
        target=request.target,
        prediction=prediction
    )
    
    return response

@app.get("/models", response_model=AvailableModelResponse)
async def list_models():
    """Lister tous les modèles de machine learning disponibles"""
    models_df = client.list_available_models()
    model_list = models_df.to_dict(orient='records')
    return AvailableModelResponse(model_list=model_list)

@app.post("/model_performance", response_model=ModelPerformanceResponse)
async def get_model_performance(model_request: ModelPerformanceRequest):
    """Récupérer les performances du modèle pour la variable cible spécifiée"""
    performance = client.get_model_performance(model_request.model, model_request.target)
    if not performance:
        raise HTTPException(status_code=404, detail="Performances du modèle non trouvées")

    last_trained = performance.get('last_trained', None)
    if isinstance(last_trained, str):
        try:
            last_trained = datetime.fromisoformat(last_trained).date()
        except ValueError:
            last_trained = None
    elif isinstance(last_trained, datetime):
        last_trained = last_trained.date()
    
    response = ModelPerformanceResponse(
        model=model_request.model,
        target=model_request.target,
        rmse=performance.get('rmse', 0.0),
        r2=performance.get('r2', 0.0),
        mae=performance.get('mae', 0.0),
        last_trained=last_trained
    )
    
    return response

@app.get("/in_production_movies", response_model=List[MovieProductionResponse])
async def get_in_production_movies():
    """Récupérer les films actuellement en production depuis la base de données"""
    engine = dbc.connect_to_db()
    movies_df = client.get_in_production_movies(engine)
    movies_list = []
    for _, row in movies_df.iterrows():
        movies_list.append(
            MovieProductionResponse(
                movie_title=row['title'],
                popularity=row['popularity'],
                budget=row['budget'],
                status=row['status'],
                release_date=row['release_date']
            )
        )
    return movies_list

@app.get("/movies/count")
async def count_movies():
    engine = dbc.connect_to_db()
    try:
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT COUNT(*) AS total FROM movies"))
            total = result.fetchone()['total']
        return {"count_movies": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur SQL: {e}")
    
@app.get("/movies/search", response_model=List[str])
async def search_movies(query: str = Query(...)):
    engine = dbc.connect_to_db()
    try:
        words = query.split()
        sql_query = "SELECT title FROM movies WHERE " + " AND ".join([f"title ILIKE '%{word}%'" for word in words])
        df = pd.read_sql(sqlalchemy.text(sql_query), engine)
        return df['title'].tolist()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur SQL: {e}")