# Use a more recent Debian base image
FROM debian:buster-slim

# Install necessary dependencies
RUN apt-get update && apt-get install -y wget openjdk-11-jdk curl gnupg

# Use a valid Spark version URL
RUN wget https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz && \
    tar xzf spark-3.2.0-bin-hadoop3.2.tgz && \
    mv spark-3.2.0-bin-hadoop3.2 /usr/local/spark && \
    rm spark-3.2.0-bin-hadoop3.2.tgz

# Set environment variables
ENV SPARK_HOME /usr/local/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Install SBT
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add && \
    apt-get update && apt-get install -y sbt

# Set the working directory
WORKDIR /app

# Copy the application files
COPY . .

# Install dependencies and build the project
RUN sbt clean compile && sbt package

# Default command with optional arguments
CMD ["sh", "-c", "sbt run ${START_DATE} ${END_DATE}"]
