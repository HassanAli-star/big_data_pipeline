# Use the official Airflow image as the base
FROM apache/airflow:2.7.0

# Install OpenJDK 11
USER root

RUN apt update && \
	apt-get update && apt-get install -y openjdk-11-jdk



# Ensure that the necessary directories are created and permissions are set
USER airflow

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH