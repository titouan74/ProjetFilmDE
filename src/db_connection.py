import time
from sqlalchemy import create_engine

def connect_to_db() -> None:
    """Établir la connexion à la base de données"""
    try:
        print("🔌 Connexion à la base de données...")
        engine = create_engine(f'postgresql://cynthia:datascientest@54.170.173.55/movie_db', 
                connect_args={"connect_timeout": 10})
        # Test de la connexion
        with engine.connect() as connection:
            connection.execute("SELECT 1")
        print("✅ Connexion réussie!")
        return engine
    except Exception as e:
        print(f"❌ Échec de la connexion à la base de données: {e}")
        exit()
    return engine
