FROM python:3.9-slim

# Set environment variables
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        wget \
        procps \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy Spark jobs
COPY jobs/ /opt/spark-jobs/

# Set working directory
WORKDIR /opt/spark-jobs

# Set default command
CMD ["bash"] 