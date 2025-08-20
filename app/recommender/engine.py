# app/recommender/engine.py
from typing import Dict, Any, List
from collections import defaultdict
import random

def recommander_videos_par_preferences(profil_utilisateur: Dict[str, Any], catalogue_cagnottes: Dict[str, Any], nombre_recommandations: int) -> List[Dict[str, Any]]:
    # ... (Copiez ici la logique de votre fonction recommander_par_preferences) ...
    pref_categories = [cat.lower() for cat in profil_utilisateur.get("categorie_dons", [])]
    pref_budget = profil_utilisateur.get("budget", {"min": 0, "max": float('inf')})
    
    scores_cagnottes = defaultdict(lambda: {"score": 0, "details": None})

    for id_cagnotte, details_cagnotte in catalogue_cagnottes.items():
        score = 0
        cagnotte_categorie = details_cagnotte.get("categorie", "").lower()
        
        if cagnotte_categorie in pref_categories:
            score += 10
        
        budget_cagnotte = details_cagnotte.get("budget_necessaire", 0)
        if pref_budget["min"] <= budget_cagnotte <= pref_budget["max"]:
            score += 5

        mots_cles_user = profil_utilisateur.get("provenance_capital", "").lower() + " " + profil_utilisateur.get("pourquoi", "").lower()
        if mots_cles_user in details_cagnotte["description"].lower() or mots_cles_user in details_cagnotte["titre"].lower():
            score += 2

        details_cagnotte['score_calculé'] = score
        scores_cagnottes[id_cagnotte] = {"score": score, "details": details_cagnotte}
    
    recommandations_triees = sorted(
        scores_cagnottes.items(),
        key=lambda item: item[1]["score"],
        reverse=True
    )

    resultats = []
    for id_cagnotte, data in recommandations_triees:
        if data["score"] > 0:
            resultats.append(data["details"])
            if len(resultats) >= nombre_recommandations:
                break
    
    if not resultats:
        resultats = random.sample(list(catalogue_cagnottes.values()), min(nombre_recommandations, len(catalogue_cagnottes)))
    
    return resultats


def recommander_videos_par_popularite(catalogue_cagnottes: Dict[str, Any], nombre_recommandations: int) -> List[Dict[str, Any]]:
    # ... (Copiez ici la logique de votre fonction recommander_par_popularite) ...
    scores_popularite = []
    for id_cagnotte, details_cagnotte in catalogue_cagnottes.items():
        score_pop = (details_cagnotte.get('nbreVue', 0) * 0.7) + (details_cagnotte.get('nbreLike', 0) * 0.3)
        scores_popularite.append((id_cagnotte, score_pop))
    
    scores_popularite.sort(key=lambda x: x[1], reverse=True)
    
    recommandations_populaires = []
    for id_cagnotte, score in scores_popularite[:nombre_recommandations]:
        reco_details = catalogue_cagnottes[id_cagnotte]
        reco_details['score_calculé'] = score
        recommandations_populaires.append(reco_details)
        
    return recommandations_populaires