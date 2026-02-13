import time
from sqlalchemy import create_engine, text
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
