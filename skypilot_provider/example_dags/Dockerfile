FROM apache/airflow:2.8.0

RUN pip install --user airflow-provider-skypilot

USER root
RUN apt update && apt install git -y
USER airflow
