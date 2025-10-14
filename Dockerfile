FROM python:3.9-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./app /app/app

# # Commande pour démarrer le serveur FastAPI avec Uvicorn
# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

CMD ["gunicorn", "app.main:app", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]

#gunicorn main:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8002