/**
 * @file
 * @brief MQTT subscribe example with localhost broker and security logging
 */
// https://cppscripts.com/paho-mqtt-cpp-cmake
#include <iostream>
#include <spdlog/common.h>
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

using json = nlohmann::json;

const std::string SERVER_ADDRESS("tcp://localhost:1883");
const std::string CLIENT_ID("ExampleSubscriber");

// === SECURITY LOGGER CLASS ===
class MQTTSecurityLogger {
private:
    std::shared_ptr<spdlog::logger> security_logger;
    std::shared_ptr<spdlog::logger> sparkplug_logger;
    std::shared_ptr<spdlog::logger> access_logger;
    std::shared_ptr<spdlog::logger> system_logger;
    
    // Security tracking
    std::unordered_map<std::string, std::vector<std::chrono::steady_clock::time_point>> client_failures;
    std::unordered_set<std::string> registered_nodes;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_birth_messages;
    std::atomic<int> command_count_per_minute{0};
    std::atomic<int> data_messages_per_minute{0};
    
public:
    MQTTSecurityLogger() {
        setup_loggers();
    }
    
    void setup_loggers() {
        try {
            // File sinks
            auto security_file = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                "logs/mqtt_security.log", 1024*1024*10, 5);
            auto sparkplug_file = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                "logs/sparkplug_events.log", 1024*1024*10, 5);
            auto access_file = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                "logs/mqtt_access.log", 1024*1024*10, 5);
            auto system_file = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                "logs/system_events.log", 1024*1024*10, 5);
            
            // Console sink
            auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            
            // Syslog sink til kritiske events
            auto syslog_sink = std::make_shared<spdlog::sinks::syslog_sink_mt>(
                "mqtt_security", LOG_PID, LOG_DAEMON, true);
            
            // Setup separate loggers
            security_logger = std::make_shared<spdlog::logger>("security", 
                spdlog::sinks_init_list{security_file, syslog_sink});
            sparkplug_logger = std::make_shared<spdlog::logger>("sparkplug", 
                spdlog::sinks_init_list{sparkplug_file, console_sink});
            access_logger = std::make_shared<spdlog::logger>("access", access_file);
            system_logger = std::make_shared<spdlog::logger>("system", 
                spdlog::sinks_init_list{system_file, console_sink});
            
            // Register loggers
            spdlog::register_logger(security_logger);
            spdlog::register_logger(sparkplug_logger);
            spdlog::register_logger(access_logger);
            spdlog::register_logger(system_logger);
            
            // Set levels
            security_logger->set_level(spdlog::level::info);
            sparkplug_logger->set_level(spdlog::level::info);
            access_logger->set_level(spdlog::level::info);
            system_logger->set_level(spdlog::level::info);
            
        } catch (const spdlog::spdlog_ex& ex) {
            std::cout << "Security logger setup failed: " << ex.what() << std::endl;
        }
    }
    
    // === SUBSCRIBER SECURITY EVENTS ===
    
    void log_subscriber_start() {
        system_logger->info("MQTT Security Subscriber starting up");
        access_logger->info("Monitoring topics for security events");
    }
    
    void log_broker_connection(const std::string& server, const std::string& client_id) {
        access_logger->info("Subscriber connected to broker - Server: {}, Client: {}", server, client_id);
        system_logger->info("Security monitoring active on broker: {}", server);
    }
    
    void log_topic_subscription(const std::string& topic) {
        access_logger->info("Subscribed to security monitoring topic: {}", topic);
    }
    
    // === SPARKPLUG MESSAGE ANALYSIS ===
    
    void analyze_nbirth_message(const std::string& topic, const std::string& payload) {
        sparkplug_logger->info("NBIRTH message received - Topic: {}", topic);
        
        std::string node_id = extract_node_from_topic(topic);
        registered_nodes.insert(node_id);
        last_birth_messages[node_id] = std::chrono::steady_clock::now();
        
        try {
            json payload_json = json::parse(payload);
            
            if (payload_json.contains("metrics")) {
                for (const auto& metric : payload_json["metrics"]) {
                    std::string metric_name = metric.value("name", "");
                    
                    // Log kritiske control metrics
                    if (metric_name.find("Emergency_stop") != std::string::npos ||
                        metric_name.find("Reboot") != std::string::npos ||
                        metric_name.find("Rebirth") != std::string::npos) {
                        
                        std::string value_str = get_metric_value_as_string(metric["value"]);
                        security_logger->info("Control metric in NBIRTH - Node: {}, Metric: {}, Value: {}", 
                                             node_id, metric_name, value_str);
                    }
                    
                    // Log hardware info
                    if (metric_name.find("Hardware") != std::string::npos) {
                        std::string hardware = get_metric_value_as_string(metric["value"]);
                        access_logger->info("Hardware registered - Node: {}, Hardware: {}", node_id, hardware);
                    }
                }
            }
            
            // Check sequence number
            if (payload_json.contains("seq")) {
                uint64_t seq = payload_json["seq"];
                sparkplug_logger->info("NBIRTH sequence - Node: {}, Seq: {}", node_id, seq);
            }
            
        } catch (const json::exception& e) {
            security_logger->error("Failed to parse NBIRTH payload - Topic: {}, Error: {}", topic, e.what());
        }
    }
    
    void analyze_ndata_message(const std::string& topic, const std::string& payload) {
        access_logger->info("NDATA message received - Topic: {}", topic);
        data_messages_per_minute++;
        
        std::string node_id = extract_node_from_topic(topic);
        
        // Check if node is registered
        if (registered_nodes.find(node_id) == registered_nodes.end()) {
            security_logger->warn("NDATA from unregistered node - Node: {}, Topic: {}", node_id, topic);
        }
        
        try {
            json payload_json = json::parse(payload);
            
            if (payload_json.contains("metrics")) {
                for (const auto& metric : payload_json["metrics"]) {
                    std::string metric_name = metric.value("name", "");
                    
                    // Monitor for abnormal sensor values
                    if (metric_name.find("Temperature") != std::string::npos && metric.contains("value")) {
                        if (metric["value"].is_number()) {
                            double temp = metric["value"].get<double>();
                            if (temp < -10.0 || temp > 60.0) {
                                security_logger->warn("Abnormal temperature reading - Node: {}, Value: {}°C", 
                                                     node_id, temp);
                            }
                        }
                    }
                    
                    if (metric_name.find("CO2") != std::string::npos && metric.contains("value")) {
                        if (metric["value"].is_number()) {
                            double co2 = metric["value"].get<double>();
                            if (co2 > 5000.0) {
                                security_logger->error("Dangerously high CO2 levels - Node: {}, Value: {} ppm", 
                                                      node_id, co2);
                            }
                        }
                    }
                    
                    // Monitor alarm status
                    if (metric_name.find("Alarms") != std::string::npos && metric.contains("value")) {
                        if (metric["value"].is_number()) {
                            uint64_t alarms = metric["value"].get<uint64_t>();
                            if (alarms > 0) {
                                security_logger->error("ALARM CONDITION - Node: {}, Alarm code: {}", 
                                                      node_id, alarms);
                            }
                        }
                    }
                }
            }
            
        } catch (const json::exception& e) {
            security_logger->error("Failed to parse NDATA payload - Topic: {}, Error: {}", topic, e.what());
        }
    }
    
    void analyze_ddata_message(const std::string& topic, const std::string& payload) {
        access_logger->info("DDATA message received - Topic: {}", topic);
        data_messages_per_minute++;
        
        try {
            json payload_json = json::parse(payload);
            
            if (payload_json.contains("metrics")) {
                for (const auto& metric : payload_json["metrics"]) {
                    std::string metric_name = metric.value("name", "");
                    
                    // Device-specific temperature monitoring
                    if (metric_name == "temperature" && metric.contains("value")) {
                        if (metric["value"].is_number()) {
                            double temp = metric["value"].get<double>();
                            std::string device_id = extract_device_from_topic(topic);
                            
                            if (temp < -10.0 || temp > 60.0) {
                                security_logger->warn("Abnormal device temperature - Device: {}, Value: {}°C", 
                                                     device_id, temp);
                            }
                        }
                    }
                }
            }
            
        } catch (const json::exception& e) {
            security_logger->error("Failed to parse DDATA payload - Topic: {}, Error: {}", topic, e.what());
        }
    }
    
    void analyze_ndeath_message(const std::string& topic, const std::string& payload) {
        std::string node_id = extract_node_from_topic(topic);
        sparkplug_logger->warn("NDEATH message received - Topic: {}, Node: {}", topic, node_id);
        
        // Check for unexpected death (short uptime)
        auto it = last_birth_messages.find(node_id);
        if (it != last_birth_messages.end()) {
            auto now = std::chrono::steady_clock::now();
            auto uptime = std::chrono::duration_cast<std::chrono::minutes>(now - it->second);
            
            if (uptime.count() < 5) {
                security_logger->error("Unexpected node death - Node: {}, Uptime: {} minutes", 
                                      node_id, uptime.count());
            } else {
                sparkplug_logger->info("Normal node shutdown - Node: {}, Uptime: {} minutes", 
                                      node_id, uptime.count());
            }
            
            last_birth_messages.erase(it);
        }
        
        registered_nodes.erase(node_id);
    }
    
    void analyze_ncmd_message(const std::string& topic, const std::string& payload) {
        std::string node_id = extract_node_from_topic(topic);
        security_logger->warn("NCMD command received - Topic: {}, Node: {}", topic, node_id);
        command_count_per_minute++;
        
        try {
            json payload_json = json::parse(payload);
            
            if (payload_json.contains("metrics")) {
                for (const auto& metric : payload_json["metrics"]) {
                    std::string metric_name = metric.value("name", "");
                    
                    // Log critical commands
                    if (metric_name.find("Emergency_stop") != std::string::npos ||
                        metric_name.find("Reboot") != std::string::npos ||
                        metric_name.find("shutdown") != std::string::npos) {
                        
                        std::string value_str = get_metric_value_as_string(metric["value"]);
                        security_logger->critical("CRITICAL COMMAND received - Node: {}, Command: {}, Value: {}", 
                                                 node_id, metric_name, value_str);
                    }
                }
            }
            
        } catch (const json::exception& e) {
            security_logger->error("Failed to parse NCMD payload - Topic: {}, Error: {}", topic, e.what());
        }
    }
    
    void analyze_dcmd_message(const std::string& topic, const std::string& payload) {
        std::string node_id = extract_node_from_topic(topic);
        std::string device_id = extract_device_from_topic(topic);
        security_logger->warn("DCMD command received - Topic: {}, Node: {}, Device: {}", 
                             topic, node_id, device_id);
        command_count_per_minute++;
    }
    
    // === CONNECTION/SUBSCRIPTION SECURITY ===
    
    void log_connection_failure(const std::string& error_msg) {
        security_logger->error("MQTT connection failed: {}", error_msg);
        system_logger->error("Subscriber connection failure - security monitoring interrupted");
    }
    
    void log_subscription_failure(const std::string& topic, const std::string& error_msg) {
        security_logger->error("Topic subscription failed - Topic: {}, Error: {}", topic, error_msg);
    }
    
    void log_disconnect() {
        access_logger->info("Subscriber disconnected from broker");
        system_logger->warn("Security monitoring stopped");
    }
    
    // === PERIODIC SECURITY CHECKS ===
    
    void perform_periodic_checks() {
        auto now = std::chrono::steady_clock::now();
        
        // Check for stale nodes (haven't sent data recently)
        for (const auto& pair : last_birth_messages) {
            auto node_age = std::chrono::duration_cast<std::chrono::minutes>(now - pair.second);
            if (node_age.count() > 60) { // 1 hour without activity
                security_logger->warn("Stale node detected - Node: {}, Last seen: {} minutes ago", 
                                     pair.first, node_age.count());
            }
        }
        
        // Check for high command frequency
        if (command_count_per_minute > 10) {
            security_logger->error("High command frequency detected - Commands/minute: {}", 
                                  command_count_per_minute.load());
        }
        
        // Check for excessive data traffic
        if (data_messages_per_minute > 1000) {
            security_logger->warn("High data message frequency - Messages/minute: {}", 
                                 data_messages_per_minute.load());
        }
        
        // Reset counters
        command_count_per_minute = 0;
        data_messages_per_minute = 0;
        
        system_logger->info("Periodic security check completed - {} registered nodes", 
                           registered_nodes.size());
    }
    
private:
    std::string extract_node_from_topic(const std::string& topic) {
        // spBv1.0/UCL-SEE-A/NBIRTH/TLab -> "TLab"
        size_t last_slash = topic.find_last_of('/');
        if (last_slash != std::string::npos && last_slash < topic.length() - 1) {
            return topic.substr(last_slash + 1);
        }
        return "unknown_node";
    }
    
    std::string extract_device_from_topic(const std::string& topic) {
        // spBv1.0/UCL-SEE-A/DDATA/TLab/VentSensor1 -> "VentSensor1"
        size_t last_slash = topic.find_last_of('/');
        if (last_slash != std::string::npos && last_slash < topic.length() - 1) {
            return topic.substr(last_slash + 1);
        }
        return "unknown_device";
    }
    
    std::string get_metric_value_as_string(const json& value) {
        if (value.is_boolean()) {
            return value.get<bool>() ? "true" : "false";
        } else if (value.is_string()) {
            return value.get<std::string>();
        } else if (value.is_number_integer()) {
            return std::to_string(value.get<int64_t>());
        } else if (value.is_number_float()) {
            return std::to_string(value.get<double>());
        }
        return "unknown_type";
    }
};

/**
 * @class MessageCallback
 * @brief Enhanced message callback with security logging
 */
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
        
        // Route message to appropriate security analysis
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
        // Setup original file logger (din originale kode bevaret)
        auto filelog = spdlog::basic_logger_mt("filelog", "logs/mqttlog.log");
        filelog->set_level(spdlog::level::debug);
        
        // Setup security logger
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
            
            // Din originale subscriptions (bevaret)
            const std::string topic_nbirth("spBv1.0/UCL-SEE-A/NBIRTH/TLab");
            client.subscribe(topic_nbirth, 0);
            security_logger.log_topic_subscription(topic_nbirth);
            
            const std::string topic_data("spBv1.0/UCL-SEE-A/DDATA/TLab/VentSensor1");
            client.subscribe(topic_data, 0);
            security_logger.log_topic_subscription(topic_data);
            
            const std::string topic_death("spBv1.0/UCL-SEE-A/NDEATH/TLab");
            client.subscribe(topic_death, 0);
            security_logger.log_topic_subscription(topic_death);
            
            // Subscribe to additional security monitoring topics
            client.subscribe("spBv1.0/+/NDATA/+", 0);  // All NDATA messages
            client.subscribe("spBv1.0/+/NCMD/+", 0);   // All command messages
            client.subscribe("spBv1.0/+/DCMD/+/+", 0); // All device commands
            
            // Start periodic security check thread
            std::thread security_thread([&security_logger]() {
                while (true) {
                    std::this_thread::sleep_for(std::chrono::minutes(1));
                    security_logger.perform_periodic_checks();
                }
            });
            
            // Wait for messages (din originale kode)
            std::this_thread::sleep_for(std::chrono::seconds(30));
            
            security_logger.log_disconnect();
            client.disconnect()->wait();
            
            // Stop security thread (if needed for longer runs)
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