#!/bin/bash

# Script pour démarrer le worker ARQ pour les analytics

echo "🚀 Démarrage du worker ARQ pour les analytics..."

# Démarrer le worker ARQ
arq feed_popular_trending_new.WorkerSettings

# Options alternatives pour la commande:
# arq worker_analytics.WorkerSettings --verbose  # Mode verbose pour plus de logs
# arq worker_analytics.WorkerSettings --health-check-interval 30  # Avec health check toutes les 30s