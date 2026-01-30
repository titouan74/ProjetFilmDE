import time
from ml.machine_learning_utils import *
from joblib import load
from db_connection import connect_to_db

from ml.machine_learning_utils import get_movie_info_from_db, predict_movie_target

if __name__ == "__main__":

    print("🚀 Bienvenue dans The Movie App !")

    # Connexion à la base de données
    engine = connect_to_db()
    
    # Sélection de la variable cible à prédire parmi ['vote_average', 'revenue', 'popularity']
    target = input("🎯 Choisissez la variable cible à prédire (vote_average, revenue, popularity): ").strip()
    print(f"📊 Variable cible sélectionnée: {target}")

    # Récupération du modèle de machine learning
    model = load(f"src/ml/models/xgb_model_{target}.joblib")
    metadata = load(f"src/ml/models/xgb_metadata_{target}.joblib")
        
    # Demander le titre du film
    movie_title = input("\n🔍 Entrez le titre du film à analyser: ")

    # Récupérer les infos du film
    movie_info = get_movie_info_from_db(movie_title, engine)

    prediction = predict_movie_target(movie_info, model, metadata)
        
    if movie_info:
        print(f"\n📋 Informations du film:")
        print(f"   Titre: {movie_info.get('title', 'N/A')}")
        print(f"   Budget: ${movie_info.get('budget', 0):,.0f}")
        print(f"   Durée: {movie_info.get('runtime', 0)} min")
        print(f"   Date: {movie_info.get('release_date', 'N/A')}")
        print(f"   Langue: {movie_info.get('original_language', 'N/A')}")
        
        if target == 'vote_average':
            print(f"   Note moyenne prédite: {prediction:.2f} / 10")
        elif target == 'revenue':
            print(f"   Revenus prédits: ${prediction:,.0f}")
        elif target == 'popularity':
            print(f"   Popularité prédite: {prediction:.2f}")