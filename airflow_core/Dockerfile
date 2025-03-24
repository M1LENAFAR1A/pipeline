# Use the official Airflow image as the base image
FROM apache/airflow:2.10.2

USER root

# Install required system dependencies for Chrome
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    chromium \
    chromium-driver \
    libgconf-2-4 \
    libnss3 \
    libxss1 \
    libasound2 \
    libxtst6 \
    xvfb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install additional Python dependencies
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
