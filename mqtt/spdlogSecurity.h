#include <iostream>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/syslog_sink.h>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <memory>
#include <chrono>
#include <string>

using json = nlohmann::json;

class MQTTSecurityLogger {
private:
    std::shared_ptr<spdlog::logger> security_logger;
    std::shared_ptr<spdlog::logger> sparkplug_logger;
    std::shared_ptr<spdlog::logger> access_logger;
    std::shared_ptr<spdlog::logger> system_logger;
    
    std::unordered_map<std::string, std::vector<std::chrono::steady_clock::time_point>> client_failures;
    std::unordered_set<std::string> registered_nodes;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_birth_messages;
    std::atomic<int> command_count_per_minute{0};
    std::atomic<int> data_messages_per_minute{0};
        
public:
        
    void setup_loggers();
    void log_subscriber_start();
    void log_broker_connection(const std::string& server, const std::string& client_id);
    void log_topic_subscription(const std::string& topic);
    void analyze_nbirth_message(const std::string& topic, const std::string& payload);
    void analyze_ndata_message(const std::string& topic, const std::string& payload);
    void analyze_ddata_message(const std::string& topic, const std::string& payload);
    void analyze_ndeath_message(const std::string& topic, const std::string& payload);    
    void analyze_ncmd_message(const std::string& topic, const std::string& payload);
    void analyze_dcmd_message(const std::string& topic, const std::string& payload);    
    void log_connection_failure(const std::string& error_msg);
    void log_subscription_failure(const std::string& topic, const std::string& error_msg);
    void log_disconnect();
    void perform_periodic_checks();

private:
    std::string extract_node_from_topic(const std::string& topic);
    std::string extract_device_from_topic(const std::string& topic);
    std::string get_metric_value_as_string(const json& value);
};