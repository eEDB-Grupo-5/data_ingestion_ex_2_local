# Start from the official Apache Airflow image
FROM apache/airflow:2.8.2

# Get the path to our requirements file
# This is where our Python script's dependencies live
COPY src/requirements.txt /requirements.txt

# Install the dependencies into Airflow's environment
RUN pip install --no-cache-dir -r /requirements.txt --verbose