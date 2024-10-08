FROM python:3.9-slim

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /postgresql-42.6.0.jar

# Install PostgreSQL
RUN apt-get update && \
    apt-get install -y postgresql postgresql-contrib && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install pyspark psycopg2-binary matplotlib pandas seaborn

# Copy SQL file
COPY f1db_postgre1.sql /data/f1db_postgre1.sql
COPY f1db_postgre2.sql /data/f1db_postgre2.sql

# Copy Python scripts
COPY main.py /app/main.py
COPY functionality1.py /app/functionality1.py
COPY functionality2.py /app/functionality2.py
COPY functionality3.py /app/functionality3.py
COPY functionality4.py /app/functionality4.py
COPY functionality5.py /app/functionality5.py
COPY functionality6.py /app/functionality6.py

# Set working directory
WORKDIR /app

# Set up PostgreSQL
USER postgres
RUN /etc/init.d/postgresql start && \
    psql --command "ALTER USER postgres WITH PASSWORD 'hunter2';" && \
    /etc/init.d/postgresql stop

USER root

# Start PostgreSQL and run your script
CMD service postgresql start && python main.py