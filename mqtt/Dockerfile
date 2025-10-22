FROM eclipse-mosquitto:latest

# Set the maintainer label
LABEL maintainer="your_name <you@examle.com>"

COPY config/mosquitto.conf /mosquitto/config/mosquitto.conf
COPY logs/mosquitto.log /mosquitto/log/mosquitto.log

# Set the entrypoint to run Mosquitto
ENTRYPOINT ["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]
