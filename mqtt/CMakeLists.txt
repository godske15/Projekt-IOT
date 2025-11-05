cmake_minimum_required(VERSION 3.10)
project(paho-sub)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(nlohmann_json REQUIRED)
find_package(Threads REQUIRED)
find_package(CURL REQUIRED)

# Add executable with ALL source files
add_executable(paho-sub 
    paho-sub.cpp
    spdlogSecurity.cpp
)

# Link libraries
target_link_libraries(paho-sub
    paho-mqttpp3
    paho-mqtt3as
    nlohmann_json::nlohmann_json
    Threads::Threads
    CURL::libcurl
    spdlog
    fmt
)

# Include directories
target_include_directories(paho-sub PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})