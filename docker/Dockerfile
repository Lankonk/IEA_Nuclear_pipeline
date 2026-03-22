# Start with a slim Python image
FROM python:3.10-slim

# Install OpenJDK (Required for PySpark's JVM) and essential build tools
RUN apt-get update && \
    apt-get install -y default-jre-headless build-essential curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the Java Home environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Set the working directory
WORKDIR /app

# Install your Python dependencies
# You can also use a requirements.txt file here if you prefer
RUN pip install --no-cache-dir \
    requests \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    fastapi \
    uvicorn \
    psycopg2-binary

# The code will be mounted dynamically via docker-compose volumes, 
# so we don't need to COPY it during the build step.