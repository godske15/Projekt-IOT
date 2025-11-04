/**
 * @file
 * @brief MQTT subscribe example with security logging
 */
#include <iostream>
#include <cstdlib>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/syslog_sink.h>
#include <mqtt/async_client.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <thread>
#include <algorithm>
#include <memory>
#include <curl/curl.h>
#include <spdlogSecurity.cpp>

using json = nlohmann::json;

const std::string SERVER_ADDRESS = "tcp://localhost:1883";  // your broker URL
const std::string CLIENT_ID = "Subscriber";

// MessageCallback class (same as before, just passing through)
class MessageCallback : public virtual mqtt::callback {
private:
    MQTTSecurityLogger* security_logger;
    
public:
    MessageCallback(MQTTSecurityLogger* logger) : security_logger(logger) {}
    
    void message_arrived(mqtt::const_message_ptr msg) override {
        std::string topic = msg->get_topic();
        std::string payload = msg->to_string();
        
        std::cout << "Message arrived: '" << payload
                  << "' on topic: " << topic << std::endl;
        
        if (topic.find("/NBIRTH/") != std::string::npos) {
            security_logger->analyze_nbirth_message(topic, payload);
        }
        else if (topic.find("/NDATA/") != std::string::npos) {
            security_logger->analyze_ndata_message(topic, payload);
        }
        else if (topic.find("/DDATA/") != std::string::npos) {
            security_logger->analyze_ddata_message(topic, payload);
        }
        else if (topic.find("/NDEATH/") != std::string::npos) {
            security_logger->analyze_ndeath_message(topic, payload);
        }
        else if (topic.find("/NCMD/") != std::string::npos) {
            security_logger->analyze_ncmd_message(topic, payload);
        }
        else if (topic.find("/DCMD/") != std::string::npos) {
            security_logger->analyze_dcmd_message(topic, payload);
        }
    }
    
    void connection_lost(const std::string& cause) override {
        security_logger->log_connection_failure("Connection lost: " + cause);
    }
};

int main() 
{
    try 
    {
        auto filelog = spdlog::basic_logger_mt("filelog", "logs/mqttlog.log");
        filelog->set_level(spdlog::level::debug);
        
        // Setup security logger with database handler
        MQTTSecurityLogger security_logger;
        security_logger.log_subscriber_start();
        
        spdlog::info("Starting MQTT subscriber with security logging...");
        
        mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
        MessageCallback cb(&security_logger);  // Pass security logger to callback
        client.set_callback(cb);
        
        mqtt::connect_options connOpts;
        connOpts.set_clean_session(true);
        
        try 
        {
            client.connect(connOpts)->wait();
            spdlog::info("Connected to the MQTT broker!");
            security_logger.log_broker_connection(SERVER_ADDRESS, CLIENT_ID);
            
            const std::string topic_nbirth("spBv1.0/UCL-SEE-A/NBIRTH/TLab");
            client.subscribe(topic_nbirth, 0);
            security_logger.log_topic_subscription(topic_nbirth);
            
            const std::string topic_data("spBv1.0/UCL-SEE-A/DDATA/TLab/VentSensor1");
            client.subscribe(topic_data, 0);
            security_logger.log_topic_subscription(topic_data);
            
            const std::string topic_death("spBv1.0/UCL-SEE-A/NDEATH/TLab");
            client.subscribe(topic_death, 0);
            security_logger.log_topic_subscription(topic_death);
            
            client.subscribe("spBv1.0/+/NDATA/+", 0);
            client.subscribe("spBv1.0/+/NCMD/+", 0);
            client.subscribe("spBv1.0/+/DCMD/+/+", 0);
            
            std::thread security_thread([&security_logger]() {
                while (true) {
                    std::this_thread::sleep_for(std::chrono::minutes(1));
                    security_logger.perform_periodic_checks();
                }
            });
            
            // Run indefinitely instead of 30 seconds
            spdlog::info("Subscriber running... Press Ctrl+C to stop");
            while (true) {
                std::this_thread::sleep_for(std::chrono::seconds(60));
            }
            
            security_logger.log_disconnect();
            client.disconnect()->wait();
            security_thread.detach();
            
        } catch (const mqtt::exception& exc) {
            spdlog::error("Error: {}", exc.what());
            filelog->error("Error: {}", exc.what());
            security_logger.log_connection_failure(exc.what());
        }
        
    } 
    catch (const spdlog::spdlog_ex& ex) 
    {
        std::cout << "Log initialization failed: " << ex.what() << std::endl;
    }
    
    return 0;
}