FROM apache/airflow:latest-python3.8
COPY ./pip-chill-requirements.txt .
RUN pip install --no-cache-dir -r pip-chill-requirements.txt
