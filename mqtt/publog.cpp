#include <iostream>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <ctime>
#include <nlohmann/json.hpp>
#include <mqtt/async_client.h>

// Tilføj spdlog
#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/syslog_sink.h>
#include <chrono>
#include <unordered_map>
#include <atomic>
#include <thread>

using json = nlohmann::json;

const std::string SERVER_ADDRESS("tcp://localhost:1883");
const std::string CLIENT_ID("ExampleClient");

// Global sequence counter (din originale kode)
static uint64_t bdSeq = 0;

uint64_t getNextSequence() {
    return ++bdSeq;
}

// === SECURITY LOGGER WRAPPER CLASS ===
class SparkplugSecurityLogger {
private:
    std::shared_ptr<spdlog::logger> security_logger;
    std::shared_ptr<spdlog::logger> sparkplug_logger;
    std::shared_ptr<spdlog::logger> access_logger;
    
    // Security tracking
    std::unordered_map<std::string, std::vector<std::chrono::steady_clock::time_point>> connection_attempts;
    std::unordered_set<std::string> registered_nodes;
    std::atomic<int> command_count_per_minute{0};
    std::chrono::steady_clock::time_point last_nbirth_time;
    
public:
    SparkplugSecurityLogger() {
        setup_loggers();
    }
    
    void setup_loggers() {
        // Setup som vi diskuterede - du kan ændre destinationerne her
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto security_file = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            "logs/sparkplug_security.log", 1024*1024*10, 5);
        auto access_file = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            "logs/sparkplug_access.log", 1024*1024*10, 5);
        auto syslog_sink = std::make_shared<spdlog::sinks::syslog_sink_mt>(
            "sparkplug_security", LOG_PID, LOG_DAEMON, true);
        
        // Security logger - kritiske events
        security_logger = std::make_shared<spdlog::logger>("security", 
            spdlog::sinks_init_list{security_file, syslog_sink});
        
        // Sparkplug logger - normale sparkplug events
        sparkplug_logger = std::make_shared<spdlog::logger>("sparkplug", 
            spdlog::sinks_init_list{access_file, console_sink});
        
        // Access logger - connection/disconnection events
        access_logger = std::make_shared<spdlog::logger>("access", access_file);
        
        spdlog::register_logger(security_logger);
        spdlog::register_logger(sparkplug_logger);
        spdlog::register_logger(access_logger);
        
        security_logger->set_level(spdlog::level::info);
        sparkplug_logger->set_level(spdlog::level::info);
        access_logger->set_level(spdlog::level::info);
    }
    
    // === LOGGING FUNCTIONS TIL DIN KODE ===
    
    void log_connection_attempt(const std::string& server, const std::string& client_id) {
        access_logger->info("MQTT Connection attempt - Server: {}, Client: {}", server, client_id);
    }
    
    void log_connection_success(const std::string& server, const std::string& client_id) {
        access_logger->info("MQTT Connected successfully - Server: {}, Client: {}", server, client_id);
        sparkplug_logger->info("Sparkplug client online - ID: {}", client_id);
    }
    
    void log_connection_failed(const std::string& server, const std::string& client_id, const std::string& error) {
        security_logger->warn("MQTT Connection failed - Server: {}, Client: {}, Error: {}", 
                             server, client_id, error);
        
        // Track failed attempts for brute force detection
        track_connection_failure(client_id);
    }
    
    void log_nbirth_publish(const std::string& topic, uint64_t sequence, const json& payload) {
        sparkplug_logger->info("NBIRTH published - Topic: {}, Sequence: {}", topic, sequence);
        
        // Extract node info fra dit topic format: spBv1.0/UCL-SEE-A/NBIRTH/TLab
        std::string node_id = extract_node_from_topic(topic);
        registered_nodes.insert(node_id);
        last_nbirth_time = std::chrono::steady_clock::now();
        
        // Log kritiske control metrics
        if (payload.contains("metrics")) {
            for (const auto& metric : payload["metrics"]) {
                std::string metric_name = metric.value("name", "");
                if (metric_name.find("Emergency_stop") != std::string::npos ||
                    metric_name.find("Reboot") != std::string::npos) {
                    security_logger->info("Control metric in NBIRTH - Node: {}, Metric: {}, Value: {}", 
                                         node_id, metric_name, metric.value("value", ""));
                }
            }
        }
    }
    
    void log_ddata_publish(const std::string& topic, uint64_t sequence, const json& payload) {
        access_logger->info("DDATA published - Topic: {}, Sequence: {}", topic, sequence);
        
        // Log sensor data hvis relevant for security
        if (payload.contains("metrics")) {
            for (const auto& metric : payload["metrics"]) {
                std::string metric_name = metric.value("name", "");
                auto value = metric.value("value", 0.0);
                
                // Log unormal værdier
                if (metric_name == "temperature" && (value < -10.0 || value > 50.0)) {
                    security_logger->warn("Abnormal temperature reading - Topic: {}, Value: {}", topic, value);
                }
            }
        }
    }
    
    void log_ndeath_publish(const std::string& topic, uint64_t sequence) {
        std::string node_id = extract_node_from_topic(topic);
        sparkplug_logger->warn("NDEATH published - Topic: {}, Node: {}, Sequence: {}", 
                              topic, node_id, sequence);
        
        // Check hvis node havde kort uptime (potentiel sikkerhedshændelse)
        auto now = std::chrono::steady_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::minutes>(now - last_nbirth_time);
        
        if (uptime.count() < 5) {
            security_logger->error("Short node uptime before death - Node: {}, Uptime: {} minutes", 
                                  node_id, uptime.count());
        }
        
        registered_nodes.erase(node_id);
    }
    
    void log_publish_success(const std::string& topic, size_t payload_size) {
        access_logger->info("Message published successfully - Topic: {}, Size: {} bytes", 
                           topic, payload_size);
    }
    
    void log_publish_failed(const std::string& topic, const std::string& error) {
        security_logger->error("Publish failed - Topic: {}, Error: {}", topic, error);
    }
    
    void log_disconnect(const std::string& client_id) {
        access_logger->info("MQTT Client disconnected - ID: {}", client_id);
        sparkplug_logger->info("Sparkplug client offline - ID: {}", client_id);
    }
    
    void log_mqtt_exception(const std::string& error_msg) {
        security_logger->error("MQTT Exception occurred: {}", error_msg);
    }
    
private:
    std::string extract_node_from_topic(const std::string& topic) {
        // For dit format: spBv1.0/UCL-SEE-A/NBIRTH/TLab -> returnerer "TLab"
        size_t last_slash = topic.find_last_of('/');
        if (last_slash != std::string::npos) {
            return topic.substr(last_slash + 1);
        }
        return "unknown";
    }
    
    void track_connection_failure(const std::string& client_id) {
        auto now = std::chrono::steady_clock::now();
        auto& attempts = connection_attempts[client_id];
        
        // Ryd gamle forsøg (ældre end 10 minutter)
        attempts.erase(
            std::remove_if(attempts.begin(), attempts.end(),
                [now](const auto& time) {
                    return std::chrono::duration_cast<std::chrono::minutes>(now - time).count() > 10;
                }), 
            attempts.end()
        );
        
        attempts.push_back(now);
        
        if (attempts.size() >= 5) {
            security_logger->critical("Multiple connection failures detected - Client: {}, Failures: {} in 10 minutes", 
                                    client_id, attempts.size());
        }
    }
};

// === DIN ORIGINALE MAIN FUNCTION MED LOGGING TILFØJET ===
int main() {
    // Instantiér security logger
    SparkplugSecurityLogger security_logger;
    
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);

    mqtt::connect_options connOpts;
    connOpts.set_clean_session(true);

    try {
        // Log connection attempt
        security_logger.log_connection_attempt(SERVER_ADDRESS, CLIENT_ID);
        
        client.connect(connOpts)->wait();
        
        // Log successful connection
        security_logger.log_connection_success(SERVER_ADDRESS, CLIENT_ID);
        
        const std::string topic_nbirth("spBv1.0/UCL-SEE-A/NBIRTH/TLab");
        json nbirth_payload;
        std::time_t timenow = std::time(nullptr);
        
        nbirth_payload["timestamp"] = timenow;
        nbirth_payload["seq"] = getNextSequence();

        // Din originale metrics kode - 100% uændret
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
        
        // Log NBIRTH publish
        security_logger.log_nbirth_publish(topic_nbirth, bdSeq, nbirth_payload);
        
        client.publish(topic_nbirth, publish_payload.data(), publish_payload.size(), 0, false);
        
        // Log successful publish
        security_logger.log_publish_success(topic_nbirth, publish_payload.size());
        
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
        
        // Log DDATA publish
        security_logger.log_ddata_publish(topic_data, bdSeq, dData_payload);
        
        client.publish(topic_data, publish_payload_data.data(), publish_payload_data.size(), 0, false);
        
        // Log successful publish
        security_logger.log_publish_success(topic_data, publish_payload_data.size());
        
        std::cout << "DDATA sent with sequence: " << bdSeq << std::endl;

        // NDEATH
        const std::string topic_ndeath("spBv1.0/UCL-SEE-A/NDEATH/TLab");
        json ndeath_payload;
        ndeath_payload["seq"] = bdSeq;
        ndeath_payload["timestamp"] = timenow;
        
        std::string publish_payload_ndeath = ndeath_payload.dump(4);
        
        // Log NDEATH publish
        security_logger.log_ndeath_publish(topic_ndeath, bdSeq);
        
        client.disconnect()->wait();
        
        // Log disconnection
        security_logger.log_disconnect(CLIENT_ID);
        
    } catch (const mqtt::exception& exc) {
        std::cerr << "Error: " << exc.what() << std::endl;
        
        // Log MQTT exception
        security_logger.log_mqtt_exception(exc.what());
    }

    return 0;
}