# from locust import HttpUser, TaskSet, task

# class UserBehavior(TaskSet):
#     @task
#     def hello_world(self):
#         self.client.get("/")

# class WebsiteUser(HttpUser):
#     tasks = [UserBehavior]
#     min_wait = 5000
#     max_wait = 15000


from locust import HttpUser, task, between
import random

COUNTRIES = ["Mali", "Cameroun", "Senegal"]

class ColdStartUser(HttpUser):
    wait_time = between(1, 3)  # temps entre les requêtes par utilisateur

    @task
    def get_combined_feed(self):
        country = random.choice(COUNTRIES)
        params = {
            "limit": 20,
            "randomize": "true"
        }
        with self.client.get(f"/cold-start/combined/feed/v1/{country}", params=params, catch_response=True) as response:
            if response.status_code == 200:
                # optionnel: vérifier que la réponse est bien une liste (JSON)
                try:
                    data = response.json()
                    if not isinstance(data, list):
                        response.failure("Payload is not a list")
                except Exception as e:
                    response.failure(f"Invalid JSON: {e}")
            else:
                response.failure(f"Status {response.status_code}")