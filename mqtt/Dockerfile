FROM eclipse-mosquitto:latest

# Set the maintainer label
LABEL maintainer="your_name <you@example.com>"

# Copy your configuration file
COPY config/mosquitto.conf /mosquitto/config/mosquitto.conf
COPY logs/ ./mosquitto/log/
RUN rm -f ./mosquitto/log/mqtt_access.log ./mosquitto/log/mqtt_security.log  ./mosquitto/log/system_events.log ./mosquitto/log/sparkplug_events.log