FROM apache/airflow:2.8.1
USER airflow
# Create .dbt directory for profiles.yml
RUN mkdir -p /home/airflow/.dbt
#install required packages
RUN pip install --no-cache-dir faker psycopg2-binary dbt-core dbt-postgres apache-airflow-providers-postgres