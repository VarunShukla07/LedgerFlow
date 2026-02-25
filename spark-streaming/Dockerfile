FROM apache/spark:3.4.1

USER root

# Install Python packages properly
RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install pyspark psycopg2-binary

# Install PostgreSQL JDBC driver permanently
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -P /opt/spark/jars/
# Optional: set working directory
WORKDIR /opt/spark-streaming

USER spark