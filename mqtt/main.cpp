#include <WiFi.h>
#include <AsyncMqttClient.h>
#include <ArduinoJson.h>
#include "ModbusMaster.h"
#include <WiFiUdp.h>

// ---------------- WIFI AP CONFIG ----------------
WiFiServer server(80);
String header;

const char* APssid = "Edge-Node-Access-Point";
const char* APpassword = "admin12345";

// Holders for inputs from AP Server
String ssid;
String password;

// AP config settings
IPAddress local_IP(192, 168, 4, 88);
IPAddress gateway(192, 168, 4, 1);
IPAddress subnet(255, 255, 255, 0);

// ---------------- MQTT CONFIG ----------------
AsyncMqttClient mqtt;

const char* MQTT_HOST = "10.132.66.217";
const uint16_t MQTT_PORT = 1883;
const char* CLIENT_ID = "Publisher";

// Handle which wifi mode we are using
enum WiFiState {
    WAIT_FOR_CREDENTIALS,
    CONNECTING_STA,
    CONNECTED_STA
};

WiFiState wifiState = WAIT_FOR_CREDENTIALS;

// Global sequence ID
static uint64_t bdSeq = 0;
uint64_t getNextSequence() { return ++bdSeq; }

// ---------------- MODBUS CONFIG ----------------
#define RX_PIN 36
#define TX_PIN 4
#define MAX485_DE 5
#define MAX485_RE_NEG 14
#define BAUD_RATE 9600
#define MODBUS_SLAVE_ID 1

// Addresses
const uint16_t ADDR_RUN_MODE      = 2;
const uint16_t ADDR_AIR_UNIT_MODE = 367;
const uint16_t ADDR_ROOM_TEMP     = 19;
const uint16_t ADDR_OUTDOOR_TEMP  = 0;
const uint16_t ADDR_ALARM_STATUS  = 70;

// Modbus buffers
uint16_t runMode     = 0;
uint16_t lastRunMode = 9999;

float indoorTemp     = 0;
float outdoorTemp    = 0;
uint16_t alarmStatus = 0;

// Create Modbus object
ModbusMaster modbus;

const char* ntpServer = "pool.ntp.org";
const long  gmtOffset_sec = 3600;
const int   daylightOffset_sec = 0;

void printLocalTime()
{
  struct tm timeinfo;
  if(!getLocalTime(&timeinfo)){
    Serial.println("Failed to obtain time");
    return;
  }
  Serial.println(&timeinfo, "%A, %B %d %Y %H:%M:%S");
}

// ------------- RS485 Direction Control -------------
void preTransmission() {
  digitalWrite(MAX485_RE_NEG, HIGH);
  digitalWrite(MAX485_DE, HIGH);
}
void postTransmission() {
  digitalWrite(MAX485_RE_NEG, LOW);
  digitalWrite(MAX485_DE, LOW);
}

// ---------------- RUN MODE TEXT MAPPING ----------------
const char* getRunModeText(uint16_t mode) {
  switch(mode) {
    case 0: return "Stopped";
    case 1: return "Starting up";
    case 2: return "Starting reduced speed";
    case 3: return "Starting full speed";
    case 4: return "Starting normal run";
    case 5: return "Normal run";
    case 6: return "Support control heating";
    case 7: return "Support control cooling";
    case 8: return "CO2 run";
    case 9: return "Night cooling";
    case 10: return "Full speed stop";
    case 11: return "Stopping fan";
    default: return "Unknown";
  }
}

// ---------------- ALARM STATUS TEXT MAPPING ----------------
const char* getAlarmText(uint16_t mode) {
  switch(mode) {
    case 0: return "Not used";
    case 1: return "Normal";
    case 2: return "Blocked";
    case 3: return "Acknowledge";
    case 4: return "Not used";
    case 5: return "Cancelled";
    case 6: return "Not used";
    case 7: return "Alarm";
    default: return "Unknown";
  }
}

// Set air unit mode (0=OFF, 1=Reduced, 2=Normal, 3=Auto) 
void setAirUnitMode(uint16_t mode) { 
    unsigned long startTime = millis(); uint8_t 
    result = modbus.writeSingleRegister(ADDR_AIR_UNIT_MODE, mode); 
    unsigned long duration = millis() - startTime;
}

// ---------------- READ MODBUS ----------------
bool readRunMode() {
  if (modbus.readInputRegisters(ADDR_RUN_MODE, 1) == modbus.ku8MBSuccess) {
    runMode = modbus.getResponseBuffer(0);
    Serial.println(runMode);
    return true;
  }
  return false;
}

bool readIndoorTemp() {
  if (modbus.readInputRegisters(ADDR_ROOM_TEMP, 1) == modbus.ku8MBSuccess) {
    indoorTemp = modbus.getResponseBuffer(0) / 10.0f;
    Serial.println(indoorTemp);
    return true;
  }
  return false;
}

bool readOutdoorTemp() {
  if (modbus.readInputRegisters(ADDR_OUTDOOR_TEMP, 1) == modbus.ku8MBSuccess) {
    outdoorTemp = modbus.getResponseBuffer(0) / 10.0f;
    Serial.println(outdoorTemp);
    return true;
  }
  return false;
}

bool readAlarmStatus() {
  if (modbus.readInputRegisters(ADDR_ALARM_STATUS, 1) == modbus.ku8MBSuccess) {
    alarmStatus = modbus.getResponseBuffer(0);
    Serial.println(alarmStatus);
    return true;
  }
  return false;
}

// ---------------- NBIRTH ----------------
void sendNBIRTH() {
  JsonDocument doc;
  time_t nowUTC = time(NULL);

  doc["timestamp"] = nowUTC;
  doc["seq"] = getNextSequence();

  JsonArray metrics = doc["metrics"].to<JsonArray>();

  // bdSeq
  JsonObject m0 = metrics.add<JsonObject>();
  m0["name"] = "bdSeq";
  m0["timestamp"] = nowUTC;
  m0["dataType"] = "UInt64";
  m0["value"] = bdSeq;

  // Node Control
  const char* ncNames[] = {
    "Node Control/Rebirth",
    "Node Control/Reboot",
    "Node Control/Emergency_stop",
    "Node Control/Maintenance_mode",
    "Node Control/Reset_alarms"
  };
  for (int i = 0; i < 5; i++) {
    JsonObject m = metrics.add<JsonObject>();
    m["name"] = ncNames[i];
    m["timestamp"] = nowUTC;
    m["dataType"] = "Boolean";
    m["value"] = false;
  }

  // Hardware
  JsonObject m6 = metrics.add<JsonObject>();
  m6["name"] = "Properties/Hardware";
  m6["timestamp"] = nowUTC;
  m6["dataType"] = "String";
  m6["value"] = "ESP32-POE";

  // Run Mode (event)
  JsonObject m7 = metrics.add<JsonObject>();
  m7["name"] = "Inputs/Run_Mode";
  m7["timestamp"] = nowUTC;
  m7["dataType"] = "String";
  m7["value"] = getRunModeText(runMode);

  // Periodic metrics (fixed)
  const char* names[] = {
    "Inputs/Indoor_temperature",
    "Inputs/Outdoor_temperature",
    "Inputs/Alarm_status"
  };

  const char* types[] = {
    "Float",
    "Float",
    "String"
  };

  for (int i = 0; i < 3; i++) {
    JsonObject m = metrics.add<JsonObject>();
    m["name"] = names[i];
    m["timestamp"] = nowUTC;
    m["dataType"] = types[i];

    if (i == 0) m["value"] = indoorTemp;
    else if (i == 1) m["value"] = outdoorTemp;
    else m["value"] = getAlarmText(alarmStatus);
  }

  String json;
  serializeJson(doc, json);
  mqtt.publish("spBv1.0/UCL-SEE-A/NBIRTH/TLab", 0, false, json.c_str());
}

// ---------------- SEND DDATA RUN MODE ----------------
void sendDDATA_RunMode() {
  JsonDocument doc;
  time_t nowUTC = time(NULL);

  doc["timestamp"] = nowUTC;
  doc["seq"] = getNextSequence();

  JsonArray metrics = doc["metrics"].to<JsonArray>();
  JsonObject m = metrics.add<JsonObject>();

  m["name"] = "Inputs/Run_Mode";
  m["timestamp"] = nowUTC;
  m["dataType"] = "String";
  m["value"] = getRunModeText(runMode);

  String json;
  serializeJson(doc, json);
  mqtt.publish("spBv1.0/UCL-SEE-A/DDATA/TLab/VentSensor1", 0, false, json.c_str());
}

// ---------------- SEND PERIODIC ----------------
void sendDDATA_Periodic() {
    JsonDocument doc;
    time_t nowUTC = time(NULL);

    doc["timestamp"] = nowUTC;
    doc["seq"] = getNextSequence();

    JsonArray metrics = doc["metrics"].to<JsonArray>();

    // Indoor temperature
    JsonObject m1 = metrics.add<JsonObject>();
    m1["name"] = "Inputs/Indoor_temperature";
    m1["timestamp"] = nowUTC;
    m1["dataType"] = "Float";
    m1["value"] = indoorTemp;

    // Outdoor temperature
    JsonObject m2 = metrics.add<JsonObject>();
    m2["name"] = "Inputs/Outdoor_temperature";
    m2["timestamp"] = nowUTC;
    m2["dataType"] = "Float";
    m2["value"] = outdoorTemp;

    // Alarm status (fixed)
    JsonObject m3 = metrics.add<JsonObject>();
    m3["name"] = "Inputs/Alarm_status";
    m3["timestamp"] = nowUTC;
    m3["dataType"] = "String";
    m3["value"] = getAlarmText(alarmStatus);

    String json;
    serializeJson(doc, json);
    mqtt.publish("spBv1.0/UCL-SEE-A/DDATA/TLab/VentSensor1", 0, false, json.c_str());
}

void lastWillMessage() {
    const char* lwm = "OlimexPublisher Device 1 gone offline";
    const char* topic = "spBv1.0/UCL-SEE-A/NDEATH/TLab";

    mqtt.setWill(topic, 1, true, lwm);
}

bool waitForNTP(unsigned long timeoutMs = 20000) {
  unsigned long start = millis();
  struct tm timeinfo;

  while (millis() - start < timeoutMs) {
    if (getLocalTime(&timeinfo)) {

      // Check if year is valid (>2020)
      if (timeinfo.tm_year + 1900 > 2021) {
        Serial.println("NTP time synchronized!");
        Serial.println(&timeinfo, "%A, %B %d %Y %H:%M:%S");
        return true;
      }
    }

    Serial.println("Waiting for NTP sync...");
    delay(500);
  }

  return false; // Failed
}

// ---------------- WIFI PORTAL ----------------
void clientConnect() {
    WiFiClient client = server.available();
    if (!client) return;

    String currentLine = "";

    while (client.connected()) {
        if (client.available()) {
            char c = client.read();
            header += c;

            if (c == '\n') {
                if (currentLine.length() == 0) {

                    if (header.indexOf("GET /set?ssid=") >= 0) {
                        int startS = header.indexOf("ssid=") + 5;
                        int endS = header.indexOf("&", startS);
                        ssid = header.substring(startS, endS);

                        int startP = header.indexOf("password=") + 9;
                        int endP = header.indexOf(" ", startP);
                        password = header.substring(startP, endP);

                        wifiState = CONNECTING_STA;
                    }

                    client.println("HTTP/1.1 200 OK");
                    client.println("Content-type:text/html");
                    client.println();
                    client.println("<html><body><h2>WiFi Setup</h2>");
                    client.println("<form action=\"/set\">SSID:<br><input name=\"ssid\" value=\"" + ssid + "\"><br>");
                    client.println("Password:<br><input name=\"password\" value=\"" + password + "\"><br><br>");
                    client.println("<button type=\"submit\">Save</button></form></body></html>");
                    break;
                }
                currentLine = "";
            }
            else if (c != '\r') {
                currentLine += c;
            }
        }
    }

    header = "";
    client.stop();
}

// ---------------- SETUP ----------------
void setup() {
    Serial.begin(9600);

    // Start AP mode
    WiFi.mode(WIFI_AP);
    WiFi.softAP(APssid, APpassword);
    WiFi.softAPConfig(local_IP, gateway, subnet);
    server.begin();

    // RS485 setup
    pinMode(MAX485_RE_NEG, OUTPUT);
    pinMode(MAX485_DE, OUTPUT);
    digitalWrite(MAX485_RE_NEG, LOW);
    digitalWrite(MAX485_DE, LOW);

    Serial2.begin(BAUD_RATE, SERIAL_8N1, RX_PIN, TX_PIN);
    modbus.begin(MODBUS_SLAVE_ID, Serial2);
    modbus.preTransmission(preTransmission);
    modbus.postTransmission(postTransmission);

    // MQTT setup
    mqtt.onConnect([](bool) {
        Serial.println("MQTT Connected!");
        sendNBIRTH();
    });

    lastWillMessage();
    mqtt.setCredentials("OlimexPublish", "PublishingEveryday");
    mqtt.setServer(MQTT_HOST, MQTT_PORT);
    mqtt.setClientId(CLIENT_ID);

    wifiState = WAIT_FOR_CREDENTIALS;

    //init and get the time
    configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);
}

// ---------------- LOOP ----------------
unsigned long lastPeriodicSend = 0;

void loop() {

  if (wifiState == WAIT_FOR_CREDENTIALS) {
      clientConnect();
    }

    else if (wifiState == CONNECTING_STA) {
    
      WiFi.mode(WIFI_STA);
      WiFi.begin(ssid.c_str(), password.c_str());

      Serial.println("Trying to connect to Wi-Fi...");

      unsigned long startAttemptTime = millis();
      while (WiFi.status() != WL_CONNECTED && millis() - startAttemptTime < 10000) {
        delay(500);
        Serial.print(".");
      }

      if (WiFi.status() == WL_CONNECTED) {
        Serial.println("\nConnected!");

        configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);

        // Wait for NTP time sync BEFORE MQTT connect
        if (!waitForNTP()) {
            Serial.println("ERROR: Failed to sync NTP time!");
            // you may want to retry or reboot here
        }

        wifiState = CONNECTED_STA;
        mqtt.connect();  // NOW it is safe
      }

      else {
        Serial.println("\nFailed to connect. Returning to AP mode.");
        wifiState = WAIT_FOR_CREDENTIALS;
        WiFi.mode(WIFI_AP);
        WiFi.softAP(APssid, APpassword);
      }
    }

    else if (wifiState == CONNECTED_STA) {

      readAlarmStatus();
      readRunMode();
      readIndoorTemp();
      readOutdoorTemp();
      setAirUnitMode(1);

      if (runMode != lastRunMode) {
          lastRunMode = runMode;
          sendDDATA_RunMode();
      }

      if (millis() - lastPeriodicSend > 10000) {
          lastPeriodicSend = millis();
          sendDDATA_Periodic();
      }
    }
}
