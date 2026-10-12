FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scanner.py .
COPY shadow_v10.py .
CMD ["python", "scanner.py"]
