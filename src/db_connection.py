import time
from sqlalchemy import create_engine, text


def connect_to_db() -> None:
    """Établir la connexion à la base de données"""
    ip_adress = "3.250.173.236"
    user = "cynthia"
    password = "datascientest"
    db = "movie_db"

    try:
        print("🔌 Connexion à la base de données...")
        engine = create_engine(f'postgresql://{user}:{password}@{ip_adress}/{db}', 
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
