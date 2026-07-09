# Airflow image with Java + Spark + dbt so the DAG's BashOperator tasks can run
# the spark jobs and dbt models in-process (LocalExecutor, single host).
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt dbt-core==1.8.9 dbt-postgres==1.8.2
