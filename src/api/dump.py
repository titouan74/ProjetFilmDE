import os
from dotenv import load_dotenv
import subprocess

load_dotenv()

PG_HOST = os.getenv("POSTGRES_HOST")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")
DUMP_FILE = r"C:\Users\User\ProjetFilmDE\src\api\movie_db_dump.sql"

PG_DUMP_PATH = r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"

# Passer le mot de passe à pg_dump via variable d'environnement temporaire
os.environ["PGPASSWORD"] = PG_PASSWORD

command = [
    PG_DUMP_PATH,
    "-U", PG_USER,
    "-h", PG_HOST,
    "-d", PG_DB
]

with open(DUMP_FILE, "w", encoding="utf-8") as f:
    result = subprocess.run(command, stdout=f, stderr=subprocess.PIPE, text=True)

if result.returncode == 0:
    print(f"✅ Dump créé avec succès dans {DUMP_FILE}")
else:
    print(f"❌ Erreur lors du dump : {result.stderr}")

del os.environ["PGPASSWORD"]