FROM python:3.9-slim

WORKDIR /app

RUN pip install pandas psycopg2-binary

COPY ingest.py ingest.py

CMD ["python", "ingest.py"]
