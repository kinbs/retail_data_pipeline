# Use the official Airflow image as the base image
FROM apache/airflow:2.9.2

# Set environment variables to ensure that Python outputs everything to the console
ENV PYTHONUNBUFFERED=1

# Install system dependencies (JDK 17 comes from apt so the image builds on any
# CPU architecture - the previous hard-coded aarch64 tarball broke x86_64 hosts).
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONPATH="/opt/airflow:/opt/airflow/scripts:${PYTHONPATH}"

# Verify Java installation
RUN java -version

# Copy the requirements file into the container
COPY requirements.txt /requirements.txt

# Switch back to airflow user before installing Python packages
USER airflow

# Install the dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Verify PySpark installation
RUN python -c "import pyspark; print(pyspark.__version__)"

# Copy the entire application folder into the container
COPY --chown=airflow:root . /opt/airflow/

# Set the default command to run Airflow webserver and scheduler
CMD ["sh", "-c", "airflow scheduler & airflow webserver"]