"""
Client pour la connexion à la base de données et le chargement des modèles ML.
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text
from joblib import load

# Import de la configuration centralisée
from . import config

def connect_to_db():
    """Établir la connexion à la base de données"""
    try:
        url = f'postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}'
        engine = create_engine(url, connect_args={"connect_timeout": 10})
        # Test de la connexion
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("✅ Connexion réussie!")
        return engine
    except Exception as e:
        print(f"❌ Échec de la connexion à la base de données: {e}")
        raise
    
def _models_dir() -> str:
    """Retourne le chemin vers le dossier des modèles ML"""
    return str(config.MODELS_DIR)

def load_model(model: str, target: str):
    """Charger le modèle de machine learning pour la variable cible spécifiée"""
    model_path = os.path.join(_models_dir(), f"{model}_model_{target}.joblib")
    try:
        model = load(model_path)
        print(f"✅ Modèle chargé pour la cible '{target}'")
        return model
    except Exception as e:
        print(f"❌ Erreur de chargement du modèle: {e}")
        return None
    
def load_metadata(model: str, target: str):
    """Charger les métadonnées du modèle pour la variable cible spécifiée"""
    metadata_path = os.path.join(_models_dir(), f"{model}_metadata_{target}.joblib")
    try:
        metadata = load(metadata_path)
        print(f"✅ Métadonnées chargées pour la cible '{target}'")
        return metadata
    except Exception as e:
        print(f"❌ Erreur de chargement des métadonnées: {e}")
        return None
    
def list_available_models() -> pd.DataFrame:
    """Lister tous les modèles disponibles avec leurs cibles"""
    models = []
    for file in os.listdir(_models_dir()):
        if file.endswith(".joblib") and "model" in file:
            parts = file.split("_")
            model_name = parts[0]
            target = parts[-1].replace(".joblib", "")
            models.append({"model": model_name, "target": target})
    return pd.DataFrame(models)

def get_model_performance(model: str, target: str) -> dict:
    """Récupérer les performances du modèle pour la variable cible spécifiée"""
    metadata = load_metadata(model, target)
    if metadata and 'model_performance' in metadata:
        performance = metadata['model_performance']
        rmse = performance.get('test_rmse', None)
        r2 = performance.get('test_r2', None)
        mae = performance.get('test_mae', None)
        last_trained = metadata.get('training_date', None)
        return {
            "model": model, 
            "target": target,
            "rmse": rmse, 
            "r2": r2, 
            "mae": mae,
            "last_trained": last_trained
        }
    else:
        print(f"❌ Performances non disponibles pour le modèle '{model}' et la cible '{target}'")
        return {}
    
def get_in_production_movies(engine) -> pd.DataFrame:
    """Récupérer les films actuellement en production depuis la base de données"""
    query = ("""
        SELECT
            title,
            popularity, 
            budget, 
            status,
            release_date 
        FROM movies 
        WHERE status like '%Production'
        ORDER BY popularity DESC
        LIMIT 10;
    """
    )
    with engine.connect() as connection:
        text_query = text(query)
        result = connection.execute(text_query)
        movies = pd.DataFrame(result.fetchall(), columns=result.keys())
    return movies