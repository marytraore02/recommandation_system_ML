#!/usr/bin/env python3
"""
Script de monitoring et gestion pour le consumer Kafka-ClickHouse
"""

import asyncio
import sys
import os
import argparse
from datetime import datetime, timedelta
import json

from clickhouse_connect import get_client
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka import TopicPartition
from dotenv import load_dotenv 

load_dotenv()

class KafkaClickHouseMonitor:
    def __init__(self):
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        self.clickhouse_db = os.getenv('CLICKHOUSE_DATABASE', 'events_db')
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.kafka_topic = os.getenv("KAFKA_TOPIC_USER_EVENT")
        
    def get_clickhouse_client(self):
        """Retourne un client ClickHouse"""
        return get_client(
            host=self.clickhouse_host,
            port=self.clickhouse_port,
            database=self.clickhouse_db
        )
    
    async def check_clickhouse_stats(self):
        """Affiche les statistiques ClickHouse"""
        try:
            client = self.get_clickhouse_client()
            
            print("=== STATISTIQUES CLICKHOUSE ===")
            
            # Nombre total d'événements
            total_events = client.query("SELECT COUNT(*) FROM user_events").result_rows[0][0]
            print(f"Total événements stockés: {total_events:,}")
            
            # Événements par type
            print("\nÉvénements par type:")
            event_types = client.query("""
                SELECT event_type, COUNT(*) as count 
                FROM user_events 
                GROUP BY event_type 
                ORDER BY count DESC
            """).result_rows
            
            for event_type, count in event_types:
                print(f"  {event_type}: {count:,}")
            
            # Événements par jour (derniers 7 jours)
            print("\nÉvénements par jour (7 derniers jours):")
            daily_stats = client.query("""
                SELECT 
                    toDate(timestamp) as date,
                    COUNT(*) as count
                FROM user_events 
                WHERE timestamp >= now() - INTERVAL 7 DAY
                GROUP BY date
                ORDER BY date DESC
            """).result_rows
            
            for date, count in daily_stats:
                print(f"  {date}: {count:,}")
            
            # Événements récents
            print(f"\nDerniers événements insérés:")
            recent = client.query("""
                SELECT event_type, timestamp, user_type, pays
                FROM user_events 
                ORDER BY inserted_at DESC 
                LIMIT 5
            """).result_rows
            
            for event_type, timestamp, user_type, pays in recent:
                print(f"  {timestamp} | {event_type} | {user_type} | {pays}")
            
            client.close()
            
        except Exception as e:
            print(f"Erreur lors de la vérification ClickHouse: {e}")
    
    async def check_kafka_stats(self):
        """Affiche les statistiques Kafka"""
        try:
            print("\n=== STATISTIQUES KAFKA ===")
            
            admin_client = AIOKafkaAdminClient(
                bootstrap_servers=self.kafka_servers
            )
            await admin_client.start()
            
            # Métadonnées du topic
            metadata = await admin_client.describe_topics([self.kafka_topic])
            topic_metadata = metadata[self.kafka_topic]
            
            print(f"Topic: {self.kafka_topic}")
            print(f"Partitions: {len(topic_metadata.partitions)}")
            
            # Consumer groups
            groups = await admin_client.list_consumer_groups()
            print(f"Consumer groups: {len(groups)}")
            
            for group in groups:
                if 'clickhouse' in group.group_id.lower():
                    print(f"  - {group.group_id}")
            
            await admin_client.close()
            
        except Exception as e:
            print(f"Erreur lors de la vérification Kafka: {e}")
    
    async def cleanup_old_data(self, days=30):
        """Supprime les données anciennes de ClickHouse"""
        try:
            client = self.get_clickhouse_client()
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            result = client.query(f"""
                SELECT COUNT(*) FROM user_events 
                WHERE timestamp < '{cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}'
            """).result_rows[0][0]
            
            if result > 0:
                print(f"Suppression de {result:,} événements antérieurs à {cutoff_date.date()}")
                
                client.command(f"""
                    DELETE FROM user_events 
                    WHERE timestamp < '{cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}'
                """)
                
                print("Suppression terminée")
            else:
                print(f"Aucun événement à supprimer antérieur à {cutoff_date.date()}")
            
            client.close()
            
        except Exception as e:
            print(f"Erreur lors du nettoyage: {e}")
    
    async def test_insertion(self):
        """Teste l'insertion d'un événement de test"""
        try:
            client = self.get_clickhouse_client()
            
            test_event = {
                'event_id': 99999,
                'event_type': 'test_event',
                'timestamp': datetime.now(),
                'user_type': 'test',
                'cagnotte_id': 'test-cagnotte',
                'video_id': 'test-video',
                'post_id': 'test-post',
                'id_categorie': 'test-category',
                'pays': 'Test',
                'user_id': 'test-user',
                'phone': '+123456789',
                'session_id': None,
                'video_duration': 60,
                'watch_duration': 30,
                'completion_rate': 0.5,
                'device': 'test',
                'video_width': None,
                'video_height': None,
                'file_size': None,
                'mime_type': None,
                'thumbnail_url': None,
                'skip_time': None,
                'raw_data': '{"test": true}'
            }
            
            client.insert('user_events', [test_event])
            print("✅ Événement de test inséré avec succès")
            
            # Vérification
            result = client.query(
                "SELECT COUNT(*) FROM user_events WHERE event_type = 'test_event'"
            ).result_rows[0][0]
            
            print(f"Nombre d'événements de test: {result}")
            
            client.close()
            
        except Exception as e:
            print(f"❌ Erreur lors du test d'insertion: {e}")

async def main():
    parser = argparse.ArgumentParser(description='Monitoring Kafka-ClickHouse Consumer')
    parser.add_argument('--stats', action='store_true', help='Afficher les statistiques')
    parser.add_argument('--cleanup', type=int, help='Nettoyer les données anciennes (jours)')
    parser.add_argument('--test', action='store_true', help='Tester l\'insertion')
    
    args = parser.parse_args()
    
    monitor = KafkaClickHouseMonitor()
    
    if args.stats:
        await monitor.check_clickhouse_stats()
        await monitor.check_kafka_stats()
    
    if args.cleanup:
        await monitor.cleanup_old_data(args.cleanup)
    
    if args.test:
        await monitor.test_insertion()
    
    if not any(vars(args).values()):
        # Mode par défaut: affichage des stats
        await monitor.check_clickhouse_stats()
        await monitor.check_kafka_stats()

if __name__ == "__main__":
    asyncio.run(main())