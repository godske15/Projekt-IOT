FROM questdb/questdb:9.0.3

USER root

# Installer build tools og dependencies
RUN apt-get update && apt-get install -y \
    g++ cmake git curl build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Installer Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs/ | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Kopiér projektet ind
WORKDIR /app
COPY . .

# Byg C++ appen
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build

# Start QuestDB og kør main-loop i baggrunden
CMD bash -c "/app/build/main & /app/bin/java -jar /app/server/target/questdb.jar"
