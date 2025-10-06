FROM questdb/questdb:9.0.3

# Install build dependencies
USER root
RUN apt-get update && apt-get install -y \
    curl \
    cmake \
    build-essential \
    pkg-config \
    git

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Set workdir
WORKDIR /app

# Copy source code into container
COPY . .

# Build your app
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build

# Copy run-loop.sh and make executable
COPY run-loop.sh /app/run-loop.sh
RUN chmod +x /app/run-loop.sh

# Start QuestDB + your loop
CMD ["/app/run-loop.sh"]
