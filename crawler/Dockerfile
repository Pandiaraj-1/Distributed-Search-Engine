# Start with official Python image
# Think of this as a clean fresh computer with Python installed
FROM python:3.11-slim

# Set working directory inside container
# Like doing "cd /app" inside the container
WORKDIR /app

# Copy requirements first (Docker caching trick)
# If requirements don't change, Docker skips reinstalling packages
COPY requirements.txt .

# Install all Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy all crawler code into container
COPY . .

# Command to run when container starts
CMD ["python", "crawler.py", "kafka"]


