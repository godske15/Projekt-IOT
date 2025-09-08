#include <iostream>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <ctime>
#include <nlohmann/json.hpp>
#include <mqtt/async_client.h>

using json = nlohmann::json;

const std::string SERVER_ADDRESS("tcp://localhost:1883");
const std::string CLIENT_ID("ExampleClient");

// Global sequence counter
static uint64_t bdSeq = 0;

uint64_t getNextSequence() {
    return ++bdSeq;
}

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
        nbirth_payload["seq"] = getNextSequence();

        // Brug individuelle indices til metrics array
        nbirth_payload["metrics"][0]["name"] = "bdSeq";
        nbirth_payload["metrics"][0]["timestamp"] = timenow;
        nbirth_payload["metrics"][0]["dataType"] = "UInt64";
        nbirth_payload["metrics"][0]["value"] = bdSeq;

        nbirth_payload["metrics"][1]["name"] = "Node Control/Rebirth";
        nbirth_payload["metrics"][1]["timestamp"] = timenow;
        nbirth_payload["metrics"][1]["dataType"] = "Boolean";
        nbirth_payload["metrics"][1]["value"] = false;

        nbirth_payload["metrics"][2]["name"] = "Node Control/Reboot";
        nbirth_payload["metrics"][2]["timestamp"] = timenow;
        nbirth_payload["metrics"][2]["dataType"] = "Boolean";
        nbirth_payload["metrics"][2]["value"] = false;

        nbirth_payload["metrics"][3]["name"] = "Node Control/Emergency_stop";
        nbirth_payload["metrics"][3]["timestamp"] = timenow;
        nbirth_payload["metrics"][3]["dataType"] = "Boolean";
        nbirth_payload["metrics"][3]["value"] = false;

        nbirth_payload["metrics"][4]["name"] = "Node Control/Maintenance_mode";
        nbirth_payload["metrics"][4]["timestamp"] = timenow;
        nbirth_payload["metrics"][4]["dataType"] = "Boolean";
        nbirth_payload["metrics"][4]["value"] = false;

        nbirth_payload["metrics"][5]["name"] = "Node Control/Reset_alarms";
        nbirth_payload["metrics"][5]["timestamp"] = timenow;
        nbirth_payload["metrics"][5]["dataType"] = "Boolean";
        nbirth_payload["metrics"][5]["value"] = false;

        nbirth_payload["metrics"][6]["name"] = "Properties/Hardware";
        nbirth_payload["metrics"][6]["timestamp"] = timenow;
        nbirth_payload["metrics"][6]["dataType"] = "String";
        nbirth_payload["metrics"][6]["value"] = "ESP32-POE";

        nbirth_payload["metrics"][7]["name"] = "Inputs/Temperature";
        nbirth_payload["metrics"][7]["timestamp"] = timenow;
        nbirth_payload["metrics"][7]["dataType"] = "Float";
        nbirth_payload["metrics"][7]["value"] = 25.5;

        nbirth_payload["metrics"][8]["name"] = "Inputs/CO2_levels";
        nbirth_payload["metrics"][8]["timestamp"] = timenow;
        nbirth_payload["metrics"][8]["dataType"] = "Float";
        nbirth_payload["metrics"][8]["value"] = 500.0;

        nbirth_payload["metrics"][9]["name"] = "Inputs/Fan_speed";
        nbirth_payload["metrics"][9]["timestamp"] = timenow;
        nbirth_payload["metrics"][9]["dataType"] = "Float";
        nbirth_payload["metrics"][9]["value"] = 30.0;

        nbirth_payload["metrics"][10]["name"] = "Inputs/Status";
        nbirth_payload["metrics"][10]["timestamp"] = timenow;
        nbirth_payload["metrics"][10]["dataType"] = "UInt64";
        nbirth_payload["metrics"][10]["value"] = 0;

        nbirth_payload["metrics"][11]["name"] = "Inputs/Alarms";
        nbirth_payload["metrics"][11]["timestamp"] = timenow;
        nbirth_payload["metrics"][11]["dataType"] = "UInt64";
        nbirth_payload["metrics"][11]["value"] = 0;

        std::string publish_payload = nbirth_payload.dump(4);
        client.publish(topic_nbirth, publish_payload.data(), publish_payload.size(), 0, false);
        
        std::cout << "NBIRTH sent with sequence: " << bdSeq << std::endl;
        
        // DDATA besked med samme tilgang
        const std::string topic_data("spBv1.0/UCL-SEE-A/DDATA/TLab/VentSensor1");
        json dData_payload;
        
        dData_payload["timestamp"] = timenow;
        dData_payload["seq"] = getNextSequence();
        
        dData_payload["metrics"][0]["name"] = "temperature";
        dData_payload["metrics"][0]["timestamp"] = timenow;
        dData_payload["metrics"][0]["dataType"] = "Float";
        dData_payload["metrics"][0]["value"] = 26.2;
        
        std::string publish_payload_data = dData_payload.dump(4);
        client.publish(topic_data, publish_payload_data.data(), publish_payload_data.size(), 0, false);
        
        std::cout << "DDATA sent with sequence: " << bdSeq << std::endl;

        // NDEATH
        const std::string topic_ndeath("spBv1.0/UCL-SEE-A/NDEATH/TLab");
        json ndeath_payload;
        ndeath_payload["seq"] = bdSeq;
        ndeath_payload["timestamp"] = timenow;
        
        std::string publish_payload_ndeath = ndeath_payload.dump(4);
        
        client.disconnect()->wait();
        
    } catch (const mqtt::exception& exc) {
        std::cerr << "Error: " << exc.what() << std::endl;
    }

    return 0;
}