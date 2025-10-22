cmake_minimum_required(VERSION 3.10)
project(mqtt_subscriber)

set(CMAKE_CXX_STANDARD 17)

find_package(nlohmann_json REQUIRED)
find_package(spdlog REQUIRED)
find_package(CURL REQUIRED)      # For libcurl HTTP post

add_executable(paho-sub paho-sub.cpp)

target_link_libraries(paho-sub
    paho-mqttpp3
    paho-mqtt3as
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    CURL::libcurl
)
