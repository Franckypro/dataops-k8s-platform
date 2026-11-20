FROM python:3.12-slim

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py /app/app.py

# Utilisateur non privilégié
USER 65532:65532
EXPOSE 8000
CMD ["python", "/app/app.py"]
