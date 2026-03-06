"""
Configuration centralisée pour l'API.
Charge les variables d'environnement et expose les settings.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Trouver le répertoire racine de l'API (src/api)
API_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = API_DIR.parent
PROJECT_ROOT = SRC_DIR.parent

# Charger le fichier .env
_env_file = API_DIR / ".env"
if _env_file.exists():
    load_dotenv(_env_file)
else:
    load_dotenv()

# Configuration base de données
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "movie_db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# Chemin vers les modèles ML
MODELS_DIR = SRC_DIR / "ml" / "models"

# API credentials
API_USER = os.getenv("API_USER")
API_PASSWORD = os.getenv("API_PASSWORD")
