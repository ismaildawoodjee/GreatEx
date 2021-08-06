FROM apache/airflow:latest-python3.8
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt