# Gunakan base image Python
FROM python:3.8-slim

# Set working directory
WORKDIR /app

# Install dependensi Python
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy aplikasi kode ke dalam kontainer
COPY . .

# Perintah yang akan dijalankan ketika kontainer dijalankan
CMD ["python", "ingest.py"]
