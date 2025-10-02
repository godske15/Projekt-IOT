'# ./QuestDB/Dockerfile

FROM debian:bookworm-slim

# Installer dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    wget \
    pkg-config \
    libssl-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*

# Installer rust (til questdb-client)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Tilføj rust til path
ENV PATH="/root/.cargo/bin:${PATH}"

# Kopiér projektet
WORKDIR /app
COPY . .
RUN rm -rf build/

# Byg projektet
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build

# Kør din main binær
CMD ["./build/main"]
