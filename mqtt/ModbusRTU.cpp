#include <ModbusMaster.h>
#include "OlimexWIFI.h"

// ================ MODBUS COMMUNICATION CONFIGURATION ================
#define RX_PIN 36           // UART2 RX pin
#define TX_PIN 4            // UART2 TX pin
#define MAX485_DE 5         // RS485 Driver Enable pin
#define MAX485_RE_NEG 14    // RS485 Receiver Enable pin (active low)
#define BAUD_RATE 9600      // Communication speed
#define MODBUS_SLAVE_ID 1   // Slave device address (adjust if needed)

// ================ MODBUS ADDRESSES ================
// All Addresses can be seen in the excel file from Jalals lections
// When choosing an address from the excel file, you need to -1 value
// So in the following addresses, 368-1 = 367 and so on
const uint16_t ADDR_AIR_UNIT_MODE = 367;  // Holding Register: System control
const uint16_t ADDR_RUN_MODE = 2;         // Input Register: Current status
const uint16_t ADDR_ROOM_TEMP = 19;       // Input Register: Room temperature

// ================ DATA BUFFERS ================
uint16_t airUnitMode = 0;
uint16_t runMode = 0;
uint16_t roomTemp = 0;
uint16_t outdoorTemp = 0;

// Create Modbus master object
ModbusMaster modbus;

// ================ RS485 Direction Control ================
void preTransmission() {
  digitalWrite(MAX485_RE_NEG, HIGH);  // Disable receiver
  digitalWrite(MAX485_DE, HIGH);      // Enable driver
}

void postTransmission() {
  digitalWrite(MAX485_RE_NEG, LOW);   // Enable receiver
  digitalWrite(MAX485_DE, LOW);       // Disable driver
}

// ================ TEXT CONVERTERS ================
// Switch case is used to easily get the description for the mode from the ventilation system

// Running mode for the ventilation system
const char* getRunModeText(uint16_t mode) {
  switch(mode) {
    case 0:  return "Stopped";
    case 1:  return "Starting up";
    case 2:  return "Starting reduced speed";
    case 3:  return "Starting full speed";
    case 4:  return "Starting normal run";
    case 5:  return "Normal run";
    case 6:  return "Support control heating";
    case 7:  return "Support control cooling";
    case 8:  return "CO2 run";
    case 9:  return "Night cooling";
    case 10: return "Full speed stop";
    case 11: return "Stopping fan";
    default: return "Unknown";
  }
}

// Set fan speed mode. Is set in the main loop
const char* getAirUnitModeText(uint16_t mode) {
  switch(mode) {
    case 0: return "Manual OFF";
    case 1: return "Manual reduced speed";
    case 2: return "Manual normal speed";
    case 3: return "Auto";
    default: return "Unknown";
  }
}

// ================ SETUP ================
void setup() {
  // Initialize RS485 control pins
  pinMode(MAX485_RE_NEG, OUTPUT);
  pinMode(MAX485_DE, OUTPUT);
  digitalWrite(MAX485_RE_NEG, LOW);
  digitalWrite(MAX485_DE, LOW);

  // Start serial communication for debugging
  Serial.begin(9600);
  delay(1000);

  // Connect to Access Point with SSID and password
  Serial.print("Setting AP...");
  WiFi.softAP(APssid, APpassword);

  IPAddress IP = WiFi.softAPIP();
  Serial.print("AP IP address: ");
  Serial.println(IP);
  server.begin();

  Serial.println("========================================");
  Serial.println("  ESP32 Ventilation System Controller");
  Serial.println("========================================");
  Serial.println();

  // Configure UART2 for Modbus communication
  Serial2.begin(BAUD_RATE, SERIAL_8N1, RX_PIN, TX_PIN);

  // Initialize Modbus master
  modbus.begin(MODBUS_SLAVE_ID, Serial2);
  modbus.preTransmission(preTransmission);
  modbus.postTransmission(postTransmission);

  Serial.println("Modbus RTU Communication Ready");
  Serial.println();
}

// ================ CONTROL FUNCTIONS ================

// Set air unit mode (0=OFF, 1=Reduced, 2=Normal, 3=Auto)
void setAirUnitMode(uint16_t mode) {
  unsigned long startTime = millis();
  uint8_t result = modbus.writeSingleRegister(ADDR_AIR_UNIT_MODE, mode);
  unsigned long duration = millis() - startTime;

  if (result == modbus.ku8MBSuccess) {
    Serial.print("Command sent: ");
    Serial.print(getAirUnitModeText(mode)); // Get's input from mode, and switch case translates to description
    Serial.print(" (value: ");
    Serial.print(mode);
    Serial.print(") - Response time: ");
    Serial.print(duration);
    Serial.println("ms");
  } else {
    Serial.print("Write Error - Code: 0x");
    Serial.print(result, HEX);
    Serial.print(" - Response time: ");
    Serial.print(duration);
    Serial.println("ms");
  }
}

// Read current run mode status
void readRunMode() {
  unsigned long startTime = millis();
  uint8_t result = modbus.readInputRegisters(ADDR_RUN_MODE, 1);
  unsigned long duration = millis() - startTime;

  if (result == modbus.ku8MBSuccess) {
    runMode = modbus.getResponseBuffer(0);
    Serial.print("  Run Mode: ");
    Serial.print(runMode);
    Serial.print(" - ");
    Serial.print(getRunModeText(runMode)); // Get's input from mode, and switch case translates to description
    Serial.print(" (");
    Serial.print(duration);
    Serial.println("ms)");
  } else {
    Serial.print("✗ Read RunMode Error - Code: 0x");
    Serial.println(result, HEX);
  }
}

// Read current control setting
void readAirUnitMode() {
  unsigned long startTime = millis();
  uint8_t result = modbus.readHoldingRegisters(ADDR_AIR_UNIT_MODE, 1);
  unsigned long duration = millis() - startTime;

  if (result == modbus.ku8MBSuccess) {
    airUnitMode = modbus.getResponseBuffer(0);
    Serial.print("  Control Mode: ");
    Serial.print(airUnitMode);
    Serial.print(" - ");
    Serial.print(getAirUnitModeText(airUnitMode)); // Get's input from mode, and switch case translates to description
    Serial.print(" (");
    Serial.print(duration);
    Serial.println("ms)");
  } else {
    Serial.print("Read AirUnitMode Error - Code: 0x");
    Serial.println(result, HEX);
  }
}

// Read room temperature
void readRoomTemperature() {
  unsigned long startTime = millis();
  uint8_t result = modbus.readInputRegisters(ADDR_ROOM_TEMP, 1);
  unsigned long duration = millis() - startTime;

  if (result == modbus.ku8MBSuccess) {
    roomTemp = modbus.getResponseBuffer(0);
    float temperature = roomTemp / 10.0f;
    Serial.print("  Room Temp: ");
    Serial.print(temperature, 1);
    Serial.print("°C (raw: ");
    Serial.print(roomTemp);
    Serial.print(") (");
    Serial.print(duration);
    Serial.println("ms)");
  } else {
    Serial.print("Read Temperature Error - Code: 0x");
    Serial.println(result, HEX);
  }
}

// ================ MAIN LOOP ================
void loop() {
  // Run function to start AP and web server
  // This is to be able to connect the ESP32-POE to the clients SSID, making it possible to be on the same network as the backend
  clientConnect();

  Serial.println("========================================");
  
  // Step 1: Set system to Manual Normal Speed
  Serial.println("COMMAND:");
  setAirUnitMode(2);  // 2 = Manual normal speed
  
  delay(500);  // Give system time to process
  
  // Step 2: Read current status
  Serial.println();
  Serial.println("CURRENT STATUS:");
  readRunMode();
  readAirUnitMode();
  readRoomTemperature();
  
  Serial.println("========================================");
  Serial.println();
  
  // Wait before next cycle
  delay(3000);
}