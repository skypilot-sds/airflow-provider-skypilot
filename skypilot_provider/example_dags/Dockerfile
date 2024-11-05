FROM apache/airflow:2.8.0

RUN pip install --user airflow-provider-skypilot

USER root
RUN apt update && apt install git=1:2.34.1-1ubuntu1.11 -y
USER airflow
