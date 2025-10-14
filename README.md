# gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8002

# locust -f locustfile.py --headless --host=http://localhost:8000 -u 1000 -r 100 -t 5m --csv=results/cold_start_1000

locust -f locustfile.py \
  --host http://127.0.0.1:8002 \
  --users 100 \
  --spawn-rate 10 \
  --run-time 2m \
  --headless \
  --csv=load_test_results
# Cela exécutera un test de 2 minutes, avec 100 utilisateurs, et exportera les résultats dans des fichiers CSV (load_test_results_*).