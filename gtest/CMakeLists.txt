cmake_minimum_required(VERSION 3.10)
project(IoT_Tests LANGUAGES CXX) // Project name and cxx = c++

set(CMAKE_CXX_STANDARD 17) // Using the c++ 17 standard
set(CMAKE_CXX_STANDARD_REQUIRED ON) // Always use this standard. Don't fall back to older versions

# Make executable. A program called test, and then a list of the file names to be executed.
add_executable(test
    gtest.cpp
)

# Include directories for Paho MQTT and PostgreSQL. Very important for the tests to run properly.
include_directories(
    /usr/include/paho-mqttpp3
    /usr/include/paho-mqtt3c
    /usr/include/postgresql
)

# Link the required libraries to tell CMake what libraries the test program depends on.
target_link_libraries(test
    paho-mqttpp3
    paho-mqtt3as
    pq
    gtest
    gtest_main
    pthread
)
