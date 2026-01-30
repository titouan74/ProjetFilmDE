from fastapi import FastAPI, Header, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional, List
import uvicorn
import pandas as pd
import base64
import sys
import os
# Ajouter le dossier parent (src) au chemin Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    release_date: str
    target: str
    prediction: float

@app.get("/health")
async def health_check():
    """Vérification de l'état de l'API"""
    return {"L'API est prête à recevoir des requêtes !"}

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
        release_date=movie_info.get('release_date', 'N/A'),
        target=request.target,
        prediction=prediction
    )
    
    return response


