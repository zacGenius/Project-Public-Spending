FROM apache/airflow:2.10.5

USER root

# Instala Java — obrigatório para o PySpark funcionar
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Define onde o Java está — sem isso o PySpark não acha o Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
# Volta para airflow — nunca rodar o processo principal como root

# Instala dependências Python extras
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt