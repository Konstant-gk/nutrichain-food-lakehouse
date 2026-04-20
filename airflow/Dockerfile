# The official Apache Airflow Docker image with Airflow, Python 3.11, and all Airflow dependencies
FROM apache/airflow:2.9.0-python3.11
USER airflow

# We COPY requirements.txt first because Docker caches each step. If requirements.txt hasn't changed, Docker skips the pip install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the src/ package into the image so the DAG can import from it.
# /opt/airflow/ is the working directory inside the official Airflow image.
COPY src/ /opt/airflow/src/