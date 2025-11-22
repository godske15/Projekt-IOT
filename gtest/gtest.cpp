#include <gtest/gtest.h>
#include <mqtt/async_client.h>
#include <libpq-fe.h>
#include <cstdlib>
#include <string>

// ======================================================================================================== //
// ================================ Flags to use when compiling in terminal =============================== //
// g++ gtest.cpp -I/usr/include/postgresql -lpaho-mqttpp3 -lpaho-mqtt3as -lpq -lgtest -lgtest_main -pthread //
// ======================================================================================================== //

bool publish(std::string message){
    const std::string SERVER_ADDRESS("tcp://localhost:1883");
    const std::string CLIENT_ID("Publisher");
    const std::string TOPIC("PublishTest");
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    mqtt::connect_options connOpts;
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts)->wait();
        client.publish(TOPIC, message.data(), message.size(), 0, false);
        client.disconnect()->wait();
        return true;
    }
    catch (...){
        return false;
    }
}

bool dbConnect() {
    const char* host = std::getenv("QUESTDB_HOST");
    const char* port = std::getenv("QUESTDB_PORT");
    if (!host) host = "127.0.0.1";
    if (!port) port = "8812";

    std::string conninfo = "host=" + std::string(host) +
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

bool fetchSensorData(int hours = 24, int limit = 100, const std::string% device_id = ""){
    const char* host = std::getenv("QUESTDB_HOST");
    const char* port = std::getenv("QUESTDB_PORT");
    if (!host) host = "127.0.0.1";
    if (!port) port = "8812";

    std::string conninfo = "host=" + std::string(host) +
                           " port=" + std::string(port) +
                           " user=admin password=quest dbname=qdb";

    PGconn* conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return false;
    }

    PGresult* res = nullptr;
    if (!device_id.empty()) {
        // Query with device_id filter
        const char* paramValues[3] = {
            std::to_string(hours).c_str(),
            std::to_string(limit).c_str(),
            device_id.c_str()
        };
        res = PQexecParams(conn,
            "SELECT timestamp, node_id, device_id, metric_name, metric_value "
            "FROM sensor_data "
            "WHERE timestamp >= dateadd('h', -$1, now()) "
            "AND device_id = $3 "
            "ORDER BY timestamp DESC "
            "LIMIT $2",
            3,          // number of params
            nullptr,    // param types
            paramValues,
            nullptr,    // param lengths
            nullptr,    // param formats
            0           // text results
        );
    } else {
        // Query without device_id
        const char* paramValues[2] = {
            std::to_string(hours).c_str(),
            std::to_string(limit).c_str()
        };
        res = PQexecParams(conn,
            "SELECT timestamp, node_id, device_id, metric_name, metric_value "
            "FROM sensor_data "
            "WHERE timestamp >= dateadd('h', -$1, now()) "
            "ORDER BY timestamp DESC "
            "LIMIT $2",
            2,          // number of params
            nullptr,
            paramValues,
            nullptr,
            nullptr,
            0
        );
    }

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "Query failed: " << PQerrorMessage(conn) << "\n";
        PQclear(res);
        PQfinish(conn);
        return false;
    }

    int rows = PQntuples(res);
    std::cout << "Fetched " << rows << " rows\n";

    // Optional: iterate and print the first row
    if (rows > 0) {
        std::cout << "First row: ";
        for (int col = 0; col < PQnfields(res); ++col) {
            std::cout << PQgetvalue(res, 0, col) << " ";
        }
        std::cout << "\n";
    }

    PQclear(res);
    PQfinish(conn);
    return true;
}

TEST(FastAPITest, QuerySensorData) {
    EXPECT_TRUE(fetchSensorData());                // default hours=24, limit=100
    EXPECT_TRUE(fetchSensorData(12, 50, "dev123")); // example with device_id
}

TEST(MQTTTest, PublishMessageTest){
    EXPECT_TRUE(publish("Hello World."));
}

TEST(FastAPITest, TestAPIToDBConnection) {
    EXPECT_TRUE(dbConnect());
}

// bool dbConnect(){
//     global pool;

//     const char* questdb_host = os.getenv('QUESTDB_HOST', '127.0.0.1');
//     const char* questdb_port = int(os.getenv('QUESTDB_PORT', '8812'));

//     try {
//         pool = await asyncpg.create_pool(
//             host=questdb_host,
//             port=questdb_port,
//             user='admin',
//             password='quest',
//             database='qdb',
//             min_size=5,
//             max_size=20
//         )
//         return true;
//     }
//     catch (...){
//         return false;
//     }
// }

// TEST(FastAPITest, TestAPIToDBConnection){
//     EXPECT_TRUE(dbConnect());
// }