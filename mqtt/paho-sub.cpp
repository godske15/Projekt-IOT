/**
 * @file
 * @brief MQTT subscribe example with localhost broker and no auth
 */

// https://cppscripts.com/paho-mqtt-cpp-cmake

#include <iostream>
#include <spdlog/common.h>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <mqtt/async_client.h>

const std::string SERVER_ADDRESS("tcp://localhost:1883");
const std::string CLIENT_ID("ExampleSubscriber");

/**
 * @class MessageCallback
 * @brief ::
 *
 */
class MessageCallback : public virtual mqtt::callback {

public:
    void message_arrived(mqtt::const_message_ptr msg) override {
        std::cout << "Message arrived: '" << msg->get_payload()
                  << "' on topic: " << msg->get_topic() << std::endl;
    }
};



int main() 
{
    try 
    {
    auto filelog = spdlog::basic_logger_mt("filelog", "logs/mqttlog.log");
    filelog->set_level(spdlog::level::debug); // Filelog
    spdlog::info("Starting MQTT subscriber..."); // Console log 
    
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    MessageCallback cb;
    client.set_callback(cb);

    mqtt::connect_options connOpts;
    connOpts.set_clean_session(true);

    try 
    {
        client.connect(connOpts)->wait();
        // std::cout << "Connected to the MQTT broker!" << std::endl; - no cout when logging
        spdlog::info("Connected to the MQTT broker!");
        
        const std::string topic("esp32/ds/temperature");
        client.subscribe(topic, 0);

        // Wait for messages
        std::this_thread::sleep_for(std::chrono::seconds(30));
        client.disconnect()->wait();
    } catch (const mqtt::exception& exc) {
        //std::cerr << "Error: " << exc.what() << std::endl;Indlæser...
        spdlog::error("Error: {}", exc.what());
        filelog->error("Error: {}", exc.what()); 
    }
    } 
    catch (const spdlog::spdlog_ex& ex) 
    {
    std::cout << "Log initialization failed: " << ex.what() << std::endl;
    }

    return 0;
}
