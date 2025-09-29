# Multi-stage build: First stage for compiling C++
FROM gcc:latest as cpp-builder

WORKDIR /build

# Install build dependencies including Rust
RUN apt-get update && apt-get install -y \
    cmake \
    curl \
    libssl-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy source code and dependencies
COPY main.cpp /build/
COPY deps/ /build/deps/

# Build the QuestDB C++ client library first
WORKDIR /build/deps/c-questdb-client
RUN cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
RUN cmake --build build --parallel

# Now compile your application with the built library
WORKDIR /build
RUN g++ -std=c++17 \
    -I/build/deps/c-questdb-client/include \
    -o questdb_client main.cpp \
    /build/deps/c-questdb-client/build/libquestdb_client.a \
    -lssl -lcrypto -lcurl -pthread

# Second stage: QuestDB base image
FROM questdb/questdb:latest

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    libcurl4 \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary
COPY --from=cpp-builder /build/questdb_client /usr/local/bin/questdb_client
RUN chmod +x /usr/local/bin/questdb_client

# Start QuestDB and run your C++ client
CMD /questdb/bin/java -cp /questdb/questdb.jar io.questdb.ServerMain & \
    sleep 10 && \
    /usr/local/bin/questdb_client localhost 9009 && \
    wait
