# Use lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install minimal system dependencies (no libatlas-base-dev)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libopenblas-dev \
    liblapack-dev \
    gfortran \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose port 8080 (required for Cloud Run)
EXPOSE 8080

# Run Streamlit in headless mode
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.headless=true"]
