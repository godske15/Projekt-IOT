// https://cppscripts.com/paho-mqtt-cpp-cmake

#include <iostream>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <ctime>
#include <nlohmann/json.hpp>
#include <mqtt/async_client.h>

using json = nlohmann::json;

const std::string SERVER_ADDRESS("tcp://localhost:1883");
const std::string CLIENT_ID("ExampleClient");

/**
 * @brief 
 *
 * @return 
 */
int main() {
  
  mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);

    mqtt::connect_options connOpts;
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts)->wait();
        std::cout << "Connected to the MQTT broker!" << std::endl;
        
        // Follow topic structure of Sparkplug B version 1.0
        const std::string topic("spBv1.0/officeb/DDATA/ventilationchamber2/olimextemp");
        // const std::string payload("Hello, MQTT!");
        // Create JSON payload written in raw JSON
        json jsonpayload = json::parse(R"(
                                   {
                                      "timestamp": 1486144502122,
                                      "metrics": [{
                                      "name": "temperature",
                                      "alias": 1,
                                      "timestamp": 1479123452194,
                                      "dataType": "integer",
                                      "value": "25.5"
                                   }],
                                      "seq": 2
                                   }
                                   )");
        // Convert JSON payload to string
        std::string payload = jsonpayload.dump(4);
        client.publish(topic, payload.data(), payload.size(), 0, false);
        std::cout << "Message published!" << std::endl;
        // You can also construct the JSON object sequentially
        json altpayload; // empty JSON structure 
        std::time_t timenow = std::time(nullptr);
        altpayload["timestamp"] = timenow;
        altpayload["metrics"]["timestamp"] = sampleTime(); // function to do
        altpayload["metrics"]["name"] = "temperature";
        altpayload["value"] = measureTemp(); // function do do
        std::string publish_payload = altpayload.dump(4);
        client.publish(topic, publish_payload.data(), publish_payload.size(), 0, false);
        client.disconnect()->wait();
    } catch (const mqtt::exception& exc) {
        std::cerr << "Error: " << exc.what() << std::endl;
    }

    return 0;
}
