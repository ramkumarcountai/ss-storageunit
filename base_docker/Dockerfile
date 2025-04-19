# Use a Debian Bookworm base image
FROM debian:bookworm

# Update apt package lists and install necessary dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    libpq-dev \
    gcc \
    stress \
    ffmpeg libsm6 libxext6 \
    nano \
    unzip \
    libgl1 \
    curl \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir --break-system-packages -r /app/requirements.txt
