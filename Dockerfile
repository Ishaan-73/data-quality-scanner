FROM python:3.10-slim

WORKDIR /app

# Install dependencies first for better caching
COPY backend/requirements.txt ./backend/
RUN pip install --no-cache-dir -r backend/requirements.txt

# Copy the entire workspace to ensure DQS package can be loaded normally
COPY . /app/

# Expose FastAPI port
EXPOSE 8000

# Run the app
WORKDIR /app/backend
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
