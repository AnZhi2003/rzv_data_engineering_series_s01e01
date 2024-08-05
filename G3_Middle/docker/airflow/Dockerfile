FROM apache/airflow:slim-2.9.2-python3.12

USER root

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    wget

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

ENTRYPOINT [ "/bin/bash", "-c" ]