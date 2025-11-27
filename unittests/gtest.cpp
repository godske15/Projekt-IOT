#include <gtest/gtest.h>
#include <mqtt/async_client.h>
#include <libpq-fe.h>
#include <cstdlib>
#include <string>
#include <chrono>
#include <iostream>

// ======================================================================= //
//  Flags to compile manually:
//
//  g++ gtest.cpp -I/usr/include/postgresql \
//      -lpaho-mqttpp3 -lpaho-mqtt3as -lpq -lgtest -lgtest_main -pthread
// ======================================================================= //

bool publish(std::string message) {
    const std::string SERVER_ADDRESS("tcp://localhost:1883");
    const std::string CLIENT_ID("Publisher");
    const std::string TOPIC("PublishTest");

    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    mqtt::connect_options connOpts;
    connOpts.set_user_name("OlimexPublish");
    connOpts.set_password("PublishingEveryday");
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts)->wait();
        client.publish(TOPIC, message.data(), message.size(), 0, false);
        client.disconnect()->wait();
        return true;
    }
    catch (...) {
        return false;
    }
}

bool dbConnect() {
    const char* host = std::getenv("QUESTDB_HOST");
    const char* port = std::getenv("QUESTDB_PORT");
    if (!host) host = "127.0.0.1";
    if (!port) port = "8812";

    std::string conninfo =
        "host=" + std::string(host) +
        " port=" + std::string(port) +
        " user=admin password=quest dbname=qdb";

    PGconn* conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return false;
    }

    PQfinish(conn);
    return true;
}

/**
 * Fetch data from a dynamically created table such as:
 *    indoor_temperature
 *
 * FastAPI ingestion ensures these tables exist.
 */
bool fetchTableData(const std::string& table_name,
                    int hours = 24,
                    int limit = 100,
                    const std::string& device_id = "")
{
    const char* host = std::getenv("QUESTDB_HOST");
    const char* port = std::getenv("QUESTDB_PORT");
    if (!host) host = "127.0.0.1";
    if (!port) port = "8812";

    std::string conninfo =
        "host=" + std::string(host) +
        " port=" + std::string(port) +
        " user=admin password=quest dbname=qdb";

    PGconn* conn = PQconnectdb(conninfo.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection failed: " << PQerrorMessage(conn) << "\n";
        PQfinish(conn);
        return false;
    }

    // -------------------------------------------------------------------
    // Compute timestamp cutoff (QuestDB expects seconds since epoch)
    // -------------------------------------------------------------------
    using namespace std::chrono;
    auto now = system_clock::now();
    auto from_time = now - hours * 1h;
    std::time_t from_unix = system_clock::to_time_t(from_time);

    // -------------------------------------------------------------------
    // Build query with or without device_id filter
    // -------------------------------------------------------------------
    PGresult* res = nullptr;

    if (!device_id.empty()) {
        const std::string time_s = std::to_string(from_unix);
        const std::string limit_s = std::to_string(limit);

        const char* paramValues[3] = {
            time_s.c_str(),
            limit_s.c_str(),
            device_id.c_str()
        };

        std::string sql =
            "SELECT timestamp, node_name, device_name, value "
            "FROM " + table_name + " "
            "WHERE timestamp >= to_timestamp($1) "
            "AND device_name = $3 "
            "ORDER BY timestamp DESC "
            "LIMIT $2";

        res = PQexecParams(
            conn, sql.c_str(),
            3, nullptr,
            paramValues,
            nullptr, nullptr, 0
        );
    }
    else {
        const std::string time_s = std::to_string(from_unix);
        const std::string limit_s = std::to_string(limit);

        const char* paramValues[2] = {
            time_s.c_str(),
            limit_s.c_str()
        };

        std::string sql =
            "SELECT timestamp, node_name, device_name, value "
            "FROM " + table_name + " "
            "WHERE timestamp >= to_timestamp($1) "
            "ORDER BY timestamp DESC "
            "LIMIT $2";

        res = PQexecParams(
            conn, sql.c_str(),
            2, nullptr,
            paramValues,
            nullptr, nullptr, 0
        );
    }

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "Query failed: " << PQerrorMessage(conn) << "\n";
        PQclear(res);
        PQfinish(conn);
        return false;
    }

    int rows = PQntuples(res);
    std::cout << "Fetched " << rows << " rows from '" << table_name << "'\n";

    if (rows > 0) {
        std::cout << "First row: ";
        for (int col = 0; col < PQnfields(res); col++) {
            std::cout << PQgetvalue(res, 0, col) << " ";
        }
        std::cout << "\n";
    }

    PQclear(res);
    PQfinish(conn);
    return true;
}

// ======================================================================= //
//                              UNIT TESTS
// ======================================================================= //

TEST(FastAPITest, QueryIndoorTemperatureDefault)
{
    // matches the table your FastAPI creates from "Inputs/Indoor_temperature"
    EXPECT_TRUE(fetchTableData("indoor_temperature"));
}

TEST(FastAPITest, QueryIndoorTemperatureWithDeviceID)
{
    EXPECT_TRUE(fetchTableData("indoor_temperature", 24, 100, "VentSensor1"));
}

TEST(MQTTTest, PublishMessageTest)
{
    EXPECT_TRUE(publish("Hello World."));
}

TEST(FastAPITest, TestAPIToDBConnection)
{
    EXPECT_TRUE(dbConnect());
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
