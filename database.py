# app/database.py
import psycopg2
import os

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST", "localhost"), # "db" est le nom du service dans docker-compose
            database=os.environ.get("DB_NAME", "postgres"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "postgres"),
            port=os.environ.get("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        print(f"Erreur de connexion à la base de données: {e}")
        return None