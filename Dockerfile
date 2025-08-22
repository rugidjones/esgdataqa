# Use lightweight Python base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for pandas, numpy, matplotlib, seaborn, etc.
RUN apt-get update && apt-get install -y \
    build-essential \
    libatlas-base-dev \
    libopenblas-dev \
    liblapack-dev \
    gfortran \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Streamlit will look at this when deployed
EXPOSE 8080

# Set environment variables so Streamlit runs correctly on GCP
ENV PORT 8080
ENV STREAMLIT_SERVER_PORT 8080
ENV STREAMLIT_SERVER_HEADLESS true
ENV STREAMLIT_SERVER_ENABLECORS false
ENV STREAMLIT_SERVER_ENABLEXSRSFPROTECTION false

# Command to run your app
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
