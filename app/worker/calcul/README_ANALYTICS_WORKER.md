# Worker ARQ pour Analytics - Documentation

## 📋 Vue d'ensemble

Ce système exécute automatiquement les analyses de popularité et tendances toutes les 24 heures via ARQ (Asynchronous Redis Queue).

### Cycle journalier :
- **Popularité** : Calcul basé sur les événements des dernières 24 heures
- **Tendances** : Comparaison entre les 24h récentes et les 48h précédentes

## 🔧 Installation

### 1. Installer ARQ

```bash
pip install arq
```

### 2. Vérifier les dépendances existantes

Assurez-vous que vous avez déjà :
- `redis`
- `clickhouse-driver`
- `python-dotenv`

## 📁 Structure des fichiers

```
votre_projet/
├── worker_analytics.py              # Worker ARQ principal
├── trigger_analytics_manually.py    # Script pour test manuel
├── start_analytics_worker.sh        # Script de démarrage
└── .env                             # Variables d'environnement
```

## ⚙️ Configuration

### Variables d'environnement (.env)

```env
# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT2=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=analytics

# Configuration Analytics (dans utils/config.py)
TABLE_NAME=user_events
```

## 🚀 Utilisation

### Démarrer le worker (Production)

```bash
# Option 1: Directement avec arq
arq worker_analytics.WorkerSettings

# Option 2: Avec le script bash
chmod +x start_analytics_worker.sh
./start_analytics_worker.sh

# Option 3: Avec plus de logs (mode verbose)
arq worker_analytics.WorkerSettings --verbose
```

### Tester manuellement (sans attendre le cron)

```bash
# Exécuter l'analyse immédiatement
python trigger_analytics_manually.py

# Ou tester directement le worker
python worker_analytics.py
```

## ⏰ Planification

La tâche est configurée pour s'exécuter :
- **Tous les jours à 2h00 du matin**
- **Au démarrage** (première exécution immédiate avec `run_at_startup=True`)

Pour modifier l'horaire, éditez dans `worker_analytics.py` :

```python
cron_jobs = [
    cron(run_analytics_task, hour=2, minute=0, run_at_startup=True)
    # Exemples d'autres planifications:
    # cron(run_analytics_task, hour=0, minute=0)  # Minuit
    # cron(run_analytics_task, hour={6, 18}, minute=0)  # 6h et 18h
    # cron(run_analytics_task, minute=0)  # Toutes les heures
]
```

## 📊 Résultats stockés dans Redis

Les clés Redis créées :

```
analytics_popularity_by_country       # Popularité par pays (expire après 25h)
analytics_popularity_by_category      # Popularité par catégorie
analytics_trending_by_country         # Tendances par pays
analytics_trending_by_category        # Tendances par catégorie
comprehensive_analytics_metadata      # Métadonnées de l'analyse
```

Expiration : **25 heures** (marge de sécurité de 1h)

## 🔍 Monitoring et Logs

### Consulter les logs du worker

Le worker affiche des logs détaillés :

```
[timestamp] INFO - 🚀 Démarrage de l'analyse analytics planifiée
[timestamp] INFO - ⬇️ Chargement des événements des 3 derniers jours depuis ClickHouse...
[timestamp] INFO - 📁 1234 événements chargés depuis ClickHouse.
[timestamp] INFO - 🌍 Calcul de la popularité par pays (24h)...
[timestamp] INFO - 📂 Calcul de la popularité par catégorie (24h)...
[timestamp] INFO - 📈 Calcul des tendances par pays (24h vs 48h)...
[timestamp] INFO - 📊 Calcul des tendances par catégorie (24h vs 48h)...
[timestamp] INFO - ✅ Résultats stockés dans Redis
[timestamp] INFO - ✅ Analyse analytics terminée avec succès
```

### Vérifier l'état du worker ARQ

```bash
# Depuis Redis CLI
redis-cli

> KEYS arq:*
> HGETALL arq:job:[job_id]
```

### Consulter les résultats dans Redis

```python
import redis
import json

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Voir la popularité par pays
data = json.loads(r.get('analytics_popularity_by_country'))
print(data)

# Voir les métadonnées
metadata = json.loads(r.get('comprehensive_analytics_metadata'))
print(f"Dernière analyse: {metadata['last_analysis']}")
```

## 🐳 Déploiement avec Docker (optionnel)

### Dockerfile pour le worker

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["arq", "worker_analytics.WorkerSettings"]
```

### docker-compose.yml

```yaml
services:
  analytics-worker:
    build: .
    container_name: analytics_worker
    env_file: .env
    depends_on:
      - redis
      - clickhouse
    restart: always
    volumes:
      - ./logs:/app/logs
```

## 🔄 Gestion du processus (Production)

### Avec systemd

Créer `/etc/systemd/system/analytics-worker.service` :

```ini
[Unit]
Description=Analytics Worker ARQ
After=network.target redis.service

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/your/project
ExecStart=/path/to/venv/bin/arq worker_analytics.WorkerSettings
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Activer et démarrer :

```bash
sudo systemctl enable analytics-worker
sudo systemctl start analytics-worker
sudo systemctl status analytics-worker

# Voir les logs
sudo journalctl -u analytics-worker -f
```

### Avec Supervisor

Configuration dans `/etc/supervisor/conf.d/analytics-worker.conf` :

```ini
[program:analytics-worker]
command=/path/to/venv/bin/arq worker_analytics.WorkerSettings
directory=/path/to/your/project
user=your_user
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/analytics-worker.log
```

```bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start analytics-worker
```

## 🧪 Tests et Debugging

### Test complet du système

```bash
# 1. Vérifier la connexion ClickHouse
python -c "from clickhouse_driver import Client; c = Client(host='localhost'); print(c.execute('SELECT 1'))"

# 2. Vérifier la connexion Redis
redis-cli ping

# 3. Tester le worker directement
python worker_analytics.py

# 4. Déclencher manuellement via ARQ
python trigger_analytics_manually.py
```

### Déboguer les erreurs

Si le worker ne démarre pas :

1. **Vérifier les variables d'environnement**
   ```bash
   python -c "import os; from dotenv import load_dotenv; load_dotenv(); print(os.getenv('REDIS_HOST'))"
   ```

2. **Vérifier les connexions**
   ```bash
   # Redis
   redis-cli ping
   
   # ClickHouse
   echo "SELECT 1" | clickhouse-client
   ```

3. **Regarder les logs détaillés**
   ```bash
   arq worker_analytics.WorkerSettings --verbose
   ```

## 📈 Personnalisation

### Modifier les poids des événements

Dans `worker_analytics.py` :

```python
self.event_weights = {
    "video_view": 1.0,
    "video_share": 5.0,  # Augmenter l'importance des partages
    "video_favorite": 3.0,
    "video_replay": 2.0,
    "video_skip": -1.0,  # Pénaliser plus les skips
    "cagnotte_detail_view": 1.5
}
```

### Modifier la décroissance temporelle

```python
self.time_decay_days = 2  # Au lieu de 1 jour
```

### Changer la fréquence d'exécution

```python
# Toutes les 12 heures
cron_jobs = [
    cron(run_analytics_task, hour={0, 12}, minute=0)
]

# Toutes les 6 heures
cron_jobs = [
    cron(run_analytics_task, hour={0, 6, 12, 18}, minute=0)
]
```

## ❓ FAQ

**Q: Comment arrêter le worker ?**
```bash
# Si démarré manuellement : Ctrl+C
# Avec systemd : sudo systemctl stop analytics-worker
# Avec supervisor : sudo supervisorctl stop analytics-worker
```

**Q: Comment voir si la tâche s'est bien exécutée ?**
```bash
# Vérifier les logs
tail -f /var/log/analytics-worker.log

# Vérifier dans Redis
redis-cli GET comprehensive_analytics_metadata
```

**Q: Que faire si une analyse échoue ?**

Le worker ARQ réessaiera automatiquement. Consultez les logs pour identifier l'erreur. Les causes fréquentes :
- Connexion ClickHouse perdue
- Redis inaccessible
- Pas de données dans la période

**Q: Peut-on exécuter plusieurs workers ?**

Oui ! ARQ supporte plusieurs workers pour la scalabilité :
```bash
# Terminal 1
arq worker_analytics.WorkerSettings

# Terminal 2
arq worker_analytics.WorkerSettings
```

## 🔒 Sécurité

- Les mots de passe Redis/ClickHouse doivent être dans `.env` (jamais committé)
- Utiliser des connexions chiffrées en production
- Limiter les permissions Redis aux clés nécessaires
- Configurer un timeout approprié pour éviter les jobs bloqués

## 📞 Support

En cas de problème :
1. Vérifier les logs du worker
2. Tester manuellement avec `trigger_analytics_manually.py`
3. Vérifier les connexions Redis/ClickHouse
4. Consulter la documentation ARQ : https://arq-docs.helpmanual.io/