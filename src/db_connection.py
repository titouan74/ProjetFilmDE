import time
from sqlalchemy import create_engine, text

ip_adress = "108.129.181.2"

def connect_to_db(ip_adress) -> None:
    """Établir la connexion à la base de données"""
    try:
        print("🔌 Connexion à la base de données...")
        engine = create_engine(f'postgresql://cynthia:datascientest@{ip_adress}/movie_db', 
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
