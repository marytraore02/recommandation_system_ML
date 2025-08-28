# Nous séparons la logique d'accès à la base de données dans ce fichier.

from typing import List, Optional, Dict, Any
from database import get_db_connection2

def get_cagnottes_from_db() -> Dict[str, Dict[str, Any]]:
    conn = get_db_connection()
    if not conn: return {}
    
    cagnottes = {}
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM cagnottes;")
        rows = cur.fetchall()
        # Créer un dictionnaire de cagnottes
        for row in rows:
            cagnotte_data = {
                "id": row[0], "titre": row[1], "description": row[2], 
                "categorie": row[3], "media_url": row[4], "nbreLike": row[5],
                "nbreVue": row[6], "nbreCommentaire": row[7], "budget_necessaire": row[8]
            }
            cagnottes[row[0]] = cagnotte_data
    conn.close()
    return cagnottes

def get_user_profile_from_db(user_id: str) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    if not conn: return None
    
    profile = None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id, nom, prenom, telephone, profession, provenance_capital, budget_min, budget_max, pourquoi, categorie_dons
            FROM user_profiles
            WHERE user_id = %s;
        """, (user_id,))
        row = cur.fetchone()
        if row:
            profile = {
                "user_id": row[0], "nom": row[1], "prenom": row[2], 
                "telephone": row[3], "profession": row[4], "provenance_capital": row[5],
                "budget": {"min": row[6], "max": row[7]}, "pourquoi": row[8],
                "categorie_dons": row[9] if row[9] else []
            }
    conn.close()
    return profile