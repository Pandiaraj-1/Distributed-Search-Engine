FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download NLTK data during build
# So it's ready when container starts
RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt')"

COPY . .

CMD ["python", "indexer.py"]

