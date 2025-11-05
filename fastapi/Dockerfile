FROM python:3.9-slim

RUN apt-get update && apt-get install -y curl

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn[standard]==0.24.0 \
    asyncpg==0.29.0

# Copy application code
COPY . .

CMD ["uvicorn", "mainapi:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]