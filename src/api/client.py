import pandas as pd
from sqlalchemy import create_engine, text
from joblib import load
    
def load_model(model: str, target: str):
    """Charger le modèle de machine learning pour la variable cible spécifiée"""
    model_path = f"../ml/models/{model}_model_{target}.joblib"
    try:
        model = load(model_path)
        print(f"✅ Modèle chargé pour la cible '{target}'")
        return model
    except Exception as e:
        print(f"❌ Erreur de chargement du modèle: {e}")
        return None
    
def load_metadata(model: str, target: str):
    """Charger les métadonnées du modèle pour la variable cible spécifiée"""
    metadata_path = f"../ml/models/{model}_metadata_{target}.joblib"
    try:
        metadata = load(metadata_path)
        print(f"✅ Métadonnées chargées pour la cible '{target}'")
        return metadata
    except Exception as e:
        print(f"❌ Erreur de chargement des métadonnées: {e}")
        return None
