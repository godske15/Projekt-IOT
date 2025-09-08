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
        const std::string topic_nbirth("spBv1.0/UCL-SEE-A/NBIRTH/TLab");
        json nbirth_payload;
        std::time_t timenow = std::time(nullptr);
        nbirth_payload["timestamp"] = timenow;

        nbirth_payload["metrics"]["name"] = "bdSeq";
        nbirth_payload["metrics"]["timestamp"] = timenow; // function to do
        nbirth_payload["metrics"]["datatype"] = "INT64";
        nbirth_payload["metrics"]["value"] = 0; // function do do

        nbirth_payload["metrics"]["name"] = "Properties/Hardware";
        nbirth_payload["metrics"]["timestamp"] = timenow; // function to do
        nbirth_payload["metrics"]["datatype"] = "String";
        nbirth_payload["metrics"]["value"] = "ESP32-POE"; // function do do

        nbirth_payload["metrics"]["name"] = "Inputs/Temperature";
        nbirth_payload["metrics"]["timestamp"] = timenow; // function to do
        nbirth_payload["metrics"]["datatype"] = "Float";
        nbirth_payload["metrics"]["value"] = 25.5; // function do do

        nbirth_payload["metrics"]["name"] = "Inputs/CO2 levels";
        nbirth_payload["metrics"]["timestamp"] = timenow; // function to do
        nbirth_payload["metrics"]["datatype"] = "Float";
        nbirth_payload["metrics"]["value"] = 25.5; // function do do

        nbirth_payload["metrics"]["name"] = "Inputs/Fan speed";
        nbirth_payload["metrics"]["timestamp"] = timenow; // function to do
        nbirth_payload["metrics"]["datatype"] = "Float";
        nbirth_payload["metrics"]["value"] = 25.5; // function do do

        // Check with switch case what the status of HVAC is
        nbirth_payload["metrics"]["name"] = "Inputs/Status";
        nbirth_payload["metrics"]["timestamp"] = timenow; // function to do
        nbirth_payload["metrics"]["datatype"] = "INT64";
        nbirth_payload["metrics"]["value"] = 0; // function do do

        // Check with switch case what the alarm from HVAC is
        nbirth_payload["metrics"]["name"] = "Inputs/Alarms";
        nbirth_payload["metrics"]["timestamp"] = timenow; // function to do
        nbirth_payload["metrics"]["datatype"] = "INT63";
        nbirth_payload["metrics"]["value"] = 0; // function do do
        
        std::string publish_payload = nbirth_payload.dump(4);
        client.publish(topic_nbirth, publish_payload.data(), publish_payload.size(), 0, false);
        
        std::cout << "Connected to the MQTT broker!" << std::endl;
        
        // Follow topic structure of Sparkplug B version 1.0
        const std::string topic_data("spBv1.0/UCL-SEE-A/DDATA/TLab/VentSensor1");

        // You can also construct the JSON object sequentially
        json dData_payload; // empty JSON structure 
        dData_payload["timestamp"] = timenow;
        dData_payload["metrics"]["name"] = "temperature";
        dData_payload["metrics"]["timestamp"] = timenow; // function to do
        dData_payload["metrics"]["datatype"] = "Float";
        dData_payload["metrics"]["value"] = 25.5; // function do do
        std::string publish_payload_data = dData_payload.dump(4);
        client.publish(topic_data, publish_payload_data.data(), publish_payload_data.size(), 0, false);

        // NDEATH only need sequence and timestamp
        // Sequence will be part of NBIRTH last will message bdSeq
        const std::string topic_ndeath("spBv1.0/UCL-SEE-A/NDEATH/TLab");
        json ndeath_payload;
        ndeath_payload["seq"] = seq;
        ndeath_payload["timestamp"] = timenow;
        std::string publish_payload_ndeath = ndeath_payload.dump(4);
        client.publish(topic_ndeath, publish_payload_ndeath.data(), publish_payload_ndeath.size(), 0, false);
        client.disconnect()->wait();
    } catch (const mqtt::exception& exc) {
        std::cerr << "Error: " << exc.what() << std::endl;
    }

    return 0;
}
