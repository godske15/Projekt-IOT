FROM questdb/questdb:9.0.3

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
