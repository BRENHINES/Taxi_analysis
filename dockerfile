FROM apache/airflow:2.3.0

USER root

# Installation de Java
RUN apt-get update && apt-get install -y openjdk-11-jdk wget

# Configuration des variables d'environnement
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=/usr/local/bin/python

# Installation de Spark
RUN mkdir -p ${SPARK_HOME} && \
    wget -q https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.3.0-bin-hadoop3.tgz -C ${SPARK_HOME} --strip-components=1 && \
    rm spark-3.3.0-bin-hadoop3.tgz

# Installation de Hadoop pour le support S3A
RUN mkdir -p ${HADOOP_HOME} && \
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz && \
    tar -xzf hadoop-3.3.1.tar.gz -C ${HADOOP_HOME} --strip-components=1 && \
    rm hadoop-3.3.1.tar.gz

# Téléchargement du driver JDBC PostgreSQL
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -O ${SPARK_HOME}/jars/postgresql-42.2.18.jar

# Téléchargement des bibliothèques AWS Hadoop pour S3A
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -O ${SPARK_HOME}/jars/hadoop-aws-3.3.1.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.11.901.jar

# Création des répertoires pour les fichiers de configuration
RUN mkdir -p /opt/spark/conf /opt/hadoop/etc/hadoop

USER airflow

# Installation des dépendances Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt