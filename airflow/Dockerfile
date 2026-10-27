# ============================================================
# Imagen de Airflow: orquestador + cliente Spark + MinIO + OM
#   - Java 17 (driver de spark-submit en modo cliente)
#   - PySpark 3.5 (mismo que el clúster standalone)
#   - jars: JDBC PostgreSQL + S3A (Hadoop AWS) para MinIO
# ============================================================
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# --- Jars para spark-submit (driver corre en este contenedor) ---
USER root
RUN mkdir -p /opt/airflow/spark/jars && chown -R airflow:0 /opt/airflow/spark
USER airflow
RUN cd /opt/airflow/spark/jars \
    && curl -fsSL -o postgresql-42.7.3.jar \
         https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    && curl -fsSL -o hadoop-aws-3.3.4.jar \
         https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o aws-java-sdk-bundle-1.12.367.jar \
         https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
    && ls -la /opt/airflow/spark/jars
