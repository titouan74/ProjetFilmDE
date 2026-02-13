import pandas as pd
from sqlalchemy import create_engine, text
from joblib import load
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_db() -> None:
    """Établir la connexion à la base de données"""
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    db = os.getenv("DB_NAME")

    try:
        print("🔌 Connexion à la base de données...")
        engine = create_engine(f'postgresql://{user}:{password}@{host}/{db}', 
                connect_args={"connect_timeout": 10})
        # Test de la connexion
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("✅ Connexion réussie!")
        return engine
    except Exception as e:
        print(f"❌ Échec de la connexion à la base de données: {e}")
        exit()
    return engine
    
def _models_dir() -> str:
    api_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(os.path.dirname(api_dir))
    return os.path.join(src_dir, "ml", "models")

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