# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt ./

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container at /app
COPY . .

# Expose the port that Streamlit runs on
EXPOSE 8080

# Run the app.py file using Streamlit and set the port
CMD ["streamlit", "run", "app.py", "--server.port", "8080"]
