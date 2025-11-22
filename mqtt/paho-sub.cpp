/**
 * @file
 * @brief MQTT subscribe example with security logging and FastAPI integration
 */
#include <iostream>
#include <cstdlib>
#include <mqtt/async_client.h>
#include <thread>
#include <algorithm>
#include <sstream>
#include <vector>
#include <tuple>
#include "spdlogSecurity.h"
#include <curl/curl.h>
#include <nlohmann/json.hpp>

const std::string SERVER_ADDRESS = "tcp://mqtt-broker:1883";
const std::string CLIENT_ID = "Subscriber";
const std::string FASTAPI_URL = "http://fastapi:8000";  // Opdater hvis nødvendigt

// Callback for CURL response
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

/**
 * Send JSON payload to FastAPI endpoint
 */
bool send_to_fastapi(const std::string& endpoint, const std::string& json_payload) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        spdlog::error("Failed to initialize CURL");
        return false;
    }
    
    std::string url = FASTAPI_URL + endpoint;
    std::string response;
    
    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_payload.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);  // 5 second timeout
    
    CURLcode res = curl_easy_perform(curl);
    
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    
    if (res != CURLE_OK) {
        spdlog::error("Failed to send to FastAPI {}: {}", endpoint, curl_easy_strerror(res));
        return false;
    }
    
    if (http_code >= 200 && http_code < 300) {
        spdlog::info("FastAPI {} success (HTTP {}): {}", endpoint, http_code, response);
        return true;
    } else {
        spdlog::error("FastAPI {} error (HTTP {}): {}", endpoint, http_code, response);
        return false;
    }
}

// MessageCallback class
class MessageCallback : public virtual mqtt::callback {
private:
    MQTTSecurityLogger* security_logger;
    
    /**
     * Parse Sparkplug B topic to extract components
     * Format: spBv1.0/{group_id}/{message_type}/{node_id}/{device_id}
     */
    std::tuple<std::string, std::string, std::string, std::string> parse_topic(const std::string& topic) {
        std::vector<std::string> parts;
        std::stringstream ss(topic);
        std::string item;
        
        while (std::getline(ss, item, '/')) {
            parts.push_back(item);
        }
        
        if (parts.size() >= 4) {
            std::string group_id = parts[1];      // UCL-SEE-A
            std::string msg_type = parts[2];      // NBIRTH, DDATA, etc.
            std::string node_id = parts[3];       // TLab
            std::string device_id = parts.size() > 4 ? parts[4] : "";  // VentSensor1 (optional)
            return {group_id, msg_type, node_id, device_id};
        }
        
        return {"", "", "", ""};
    }
    
public:
    MessageCallback(MQTTSecurityLogger* logger) : security_logger(logger) {}
    
    void message_arrived(mqtt::const_message_ptr msg) override {
        std::string topic = msg->get_topic();
        std::string payload = msg->to_string();
        
        std::cout << "Message arrived on topic: " << topic << std::endl;
        std::cout << "Payload: " << payload << std::endl;
        
        auto [group_id, msg_type, node_id, device_id] = parse_topic(topic);
        
        // Handle different message types
        if (topic.find("/NBIRTH/") != std::string::npos) {
            spdlog::info("Processing NBIRTH message for node: {}", node_id);
            security_logger->analyze_nbirth_message(topic, payload);
            
            // Send to FastAPI
            std::string endpoint = "/ingest/nbirth/" + group_id + "/" + node_id;
            if (send_to_fastapi(endpoint, payload)) {
                spdlog::info("NBIRTH data successfully sent to database");
            }
        }
        else if (topic.find("/DDATA/") != std::string::npos) {
            spdlog::info("Processing DDATA message for device: {}/{}", node_id, device_id);
            security_logger->analyze_ddata_message(topic, payload);
            
            // Send to FastAPI
            std::string endpoint = "/ingest/ddata/" + group_id + "/" + node_id + "/" + device_id;
            if (send_to_fastapi(endpoint, payload)) {
                spdlog::info("DDATA data successfully sent to database");
            }
        }
        else if (topic.find("/NDATA/") != std::string::npos) {
            spdlog::info("Processing NDATA message for node: {}", node_id);
            security_logger->analyze_ndata_message(topic, payload);
            
            // TODO: Add NDATA endpoint if needed
            // std::string endpoint = "/ingest/ndata/" + group_id + "/" + node_id;
            // send_to_fastapi(endpoint, payload);
        }
        else if (topic.find("/NDEATH/") != std::string::npos) {
            spdlog::info("Processing NDEATH message for node: {}", node_id);
            security_logger->analyze_ndeath_message(topic, payload);
            
            // TODO: Add NDEATH endpoint if needed
            // std::string endpoint = "/ingest/ndeath/" + group_id + "/" + node_id;
            // send_to_fastapi(endpoint, payload);
        }
        else if (topic.find("/NCMD/") != std::string::npos) {
            spdlog::info("Processing NCMD message for node: {}", node_id);
            security_logger->analyze_ncmd_message(topic, payload);
        }
        else if (topic.find("/DCMD/") != std::string::npos) {
            spdlog::info("Processing DCMD message for device: {}/{}", node_id, device_id);
            security_logger->analyze_dcmd_message(topic, payload);
        }
        else {
            spdlog::warn("Unknown message type on topic: {}", topic);
        }
    }
    
    void connection_lost(const std::string& cause) override {
        spdlog::error("Connection lost: {}", cause);
        security_logger->log_connection_failure("Connection lost: " + cause);
    }
};

int main() 
{
    try 
    {
        // Initialize CURL globally
        curl_global_init(CURL_GLOBAL_ALL);
        
        auto filelog = spdlog::basic_logger_mt("filelog", "logs/mqttlog.log");
        filelog->set_level(spdlog::level::debug);
        
        // Setup security logger with database handler
        MQTTSecurityLogger security_logger;
        security_logger.setup_loggers();
        security_logger.log_subscriber_start();
        
        spdlog::info("Starting MQTT subscriber with security logging and FastAPI integration...");
        spdlog::info("FastAPI URL: {}", FASTAPI_URL);
        
        mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
        MessageCallback cb(&security_logger);
        client.set_callback(cb);
        
        mqtt::connect_options connOpts;
        connOpts.set_clean_session(true);
        
        try 
        {
            client.connect(connOpts)->wait();
            spdlog::info("Connected to the MQTT broker!");
            security_logger.log_broker_connection(SERVER_ADDRESS, CLIENT_ID);
            
            // Subscribe to specific topics
            const std::string topic_nbirth("spBv1.0/UCL-SEE-A/NBIRTH/TLab");
            client.subscribe(topic_nbirth, 0);
            security_logger.log_topic_subscription(topic_nbirth);
            spdlog::info("Subscribed to: {}", topic_nbirth);
            
            const std::string topic_data("spBv1.0/UCL-SEE-A/DDATA/TLab/VentSensor1");
            client.subscribe(topic_data, 0);
            security_logger.log_topic_subscription(topic_data);
            spdlog::info("Subscribed to: {}", topic_data);
            
            const std::string topic_death("spBv1.0/UCL-SEE-A/NDEATH/TLab");
            client.subscribe(topic_death, 0);
            security_logger.log_topic_subscription(topic_death);
            spdlog::info("Subscribed to: {}", topic_death);
            
            // Subscribe to wildcard topics
            client.subscribe("spBv1.0/+/NDATA/+", 0);
            spdlog::info("Subscribed to: spBv1.0/+/NDATA/+");
            
            client.subscribe("spBv1.0/+/NCMD/+", 0);
            spdlog::info("Subscribed to: spBv1.0/+/NCMD/+");
            
            client.subscribe("spBv1.0/+/DCMD/+/+", 0);
            spdlog::info("Subscribed to: spBv1.0/+/DCMD/+/+");
            
            // Start periodic security checks thread
            std::thread security_thread([&security_logger]() {
                while (true) {
                    std::this_thread::sleep_for(std::chrono::minutes(1));
                    security_logger.perform_periodic_checks();
                }
            });
            
            // Run indefinitely
            spdlog::info("Subscriber running... Press Ctrl+C to stop");
            spdlog::info("Waiting for messages...");
            
            while (true) {
                std::this_thread::sleep_for(std::chrono::seconds(60));
            }
            
            // Cleanup (won't be reached without signal handling)
            security_logger.log_disconnect();
            client.disconnect()->wait();
            security_thread.detach();
            
        } catch (const mqtt::exception& exc) {
            spdlog::error("MQTT Error: {}", exc.what());
            filelog->error("MQTT Error: {}", exc.what());
            security_logger.log_connection_failure(exc.what());
        }
        
        // Cleanup CURL
        curl_global_cleanup();
        
    } 
    catch (const spdlog::spdlog_ex& ex) 
    {
        std::cout << "Log initialization failed: " << ex.what() << std::endl;
    }
    
    return 0;
}