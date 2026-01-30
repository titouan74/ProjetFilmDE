from datetime import datetime
import time
from joblib import dump
from machine_learning_utils import *
import sys
import os
# Ajouter le dossier parent (src) au chemin Python pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import connect_to_db


if __name__ == "__main__":
    # Début du timer
    start_time = time.time()

    # Connexion à la base de données
    engine = connect_to_db()

    # Définition des variables cibles
    targets = ["revenue", "popularity", "vote_average"]

    for target in targets:
        print("\n" + "="*50)
        print(f"🎯 TRAITEMENT POUR LA VARIABLE CIBLE: {target.upper()}")
        print("="*50)   

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
            print("🤖 ENTRAÎNEMENT DES MODÈLES")
            print("="*50)
            
            # Définition des modèles à entraîner
            models_config = [
                {
                    'name': 'XGBoost',
                    'function': process_xgb_regressor,
                    'prefix': 'xgb'
                },
                {
                    'name': 'Linear Regression',
                    'function': process_linear_regression,
                    'prefix': 'lr'
                },
                {
                    'name': 'Random Forest',
                    'function': process_random_forest,
                    'prefix': 'rf'
                }
            ]
            
            # Création du répertoire models si nécessaire
            model_dir = "src/ml/models"
            os.makedirs(model_dir, exist_ok=True)
            
            # Entraînement et sauvegarde de chaque modèle
            for model_config in models_config:
                print(f"\n🔧 Entraînement du modèle {model_config['name']}...")
                
                # Entraînement du modèle
                model, X_train, X_test, y_train, y_test = model_config['function'](X, y)
                
                # Sauvegarde du modèle
                print(f"💾 Sauvegarde du modèle {model_config['name']}...")
                
                model_filename = f"{model_dir}/{model_config['prefix']}_model_{target}.joblib"
                dump(model, model_filename)
                
                # Création et sauvegarde des métadonnées complètes
                metadata = {
                    'target': target,
                    'model_type': model_config['name'],
                    'training_date': datetime.now().isoformat(),
                    'train_score': float(model.score(X_train, y_train)),
                    'test_score': float(model.score(X_test, y_test)),
                    'n_features': X.shape[1],
                    'n_samples': X.shape[0],
                    'feature_columns': list(X.columns),
                    'model_params': model.get_params(),
                    'data_info': {
                        'train_samples': X_train.shape[0],
                        'test_samples': X_test.shape[0],
                        'target_mean': float(y.mean()),
                        'target_std': float(y.std()),
                        'target_min': float(y.min()),
                        'target_max': float(y.max())
                    }
                }
                
                metadata_filename = f"{model_dir}/{model_config['prefix']}_metadata_{target}.joblib"
                dump(metadata, metadata_filename)
                
                # Affichage des informations de sauvegarde
                print(f"✅ Modèle {model_config['name']} sauvegardé: {model_filename}")
                print(f"✅ Métadonnées sauvegardées: {metadata_filename}")
                print(f"📊 Scores - Train: {metadata['train_score']:.4f}, Test: {metadata['test_score']:.4f}")
                print(f"🔧 Features: {metadata['n_features']}, Échantillons: {metadata['n_samples']}")
                
            print(f"\n🎉 Tous les modèles ont été entraînés et sauvegardés avec succès pour {target}!")
        
    # Fin du timer
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("\n✅ Entrainement terminé pour toutes les variables cibles.")
    print(f"\n⏱️ Temps total d'exécution du script: {elapsed_time:.2f} secondes")