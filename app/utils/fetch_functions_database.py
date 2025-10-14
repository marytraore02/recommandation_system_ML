import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import select, distinct
from db.postgres_config import get_db
from pydentics.models import CagnotteModel


async def fetch_countries_from_db():
    """Récupère la liste unique des pays depuis la table Cagnotte."""
    print("🔄 Chargement des pays depuis la base de données...")
    async for db in get_db():
        result = await db.execute(select(distinct(CagnotteModel.pays)))
        countries = [row[0] for row in result.fetchall() if row[0]]
        return countries