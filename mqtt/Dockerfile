FROM eclipse-mosquitto:latest

# Set the maintainer label
LABEL maintainer="your_name <you@example.com>"

# Copy your configuration file
COPY config/mosquitto.conf /mosquitto/config/mosquitto.conf

# Ensure proper permissions for data and log directories
USER root
RUN mkdir -p /mosquitto/data /mosquitto/log && \
    chown -R mosquitto:mosquitto /mosquitto/data /mosquitto/log && \
    chmod -R 755 /mosquitto/data /mosquitto/log

# Switch back to mosquitto user for security
USER mosquitto