#!/bin/bash

# Script pour démarrer le worker ARQ pour les analytics
# Ce worker exécutera automatiquement la tâche d'analyse toutes les 24h à 2h du matin

echo "🚀 Démarrage du worker ARQ pour les analytics..."

# Activer l'environnement virtuel si nécessaire
# source venv/bin/activate

# Démarrer le worker ARQ
arq calcul_popular_trending.WorkerSettings

# Options alternatives pour la commande:
# arq worker_analytics.WorkerSettings --verbose  # Mode verbose pour plus de logs
# arq worker_analytics.WorkerSettings --health-check-interval 30  # Avec health check toutes les 30s