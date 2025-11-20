#include <WiFi.h>

// ================ Local Web Server =================
const char* APssid = "Edge-Node-Access-Point";
const char* APpassword = "admin";

// Set Web server port number to 80
WiFiServer server(80);

// Variable to store the HTTP request
String header;

// Variables to hold input from webserver
String ssid;
String password;

void clientConnect() {
  WiFiClient client = server.available();

  if (client) {
    Serial.println("New Client.");
    String currentLine = "";

    while (client.connected()) {
      if (client.available()) {
        char c = client.read();
        header += c;

        if (c == '\n') {

          if (currentLine.length() == 0) {

            // ======== Pars request for SSID og Password ========
            if (header.indexOf("GET /set?ssid=") >= 0) {

              int ssidStart = header.indexOf("ssid=") + 5;
              int ssidEnd = header.indexOf("&", ssidStart);
              ssid = header.substring(ssidStart, ssidEnd);

              int passStart = header.indexOf("password=") + 9;
              int passEnd = header.indexOf(" ", passStart);
              password = header.substring(passStart, passEnd);

              Serial.println("Received SSID: " + ssid);
              Serial.println("Received Password: " + password);
            }

            // ======== Send HTML side ========
            client.println("HTTP/1.1 200 OK");
            client.println("Content-type:text/html");
            client.println("Connection: close");
            client.println();

            client.println("<!DOCTYPE html><html>");
            client.println("<head><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
            client.println("<style>");
            client.println("html { font-family: Helvetica; text-align:center; }");
            client.println("input{font-size:20px;padding:10px;margin:10px;width:80%;}");
            client.println("button{padding:10px 20px;font-size:22px;margin-top:20px;}");
            client.println("</style>");
            client.println("</head><body>");

            client.println("<h1>WiFi Setup</h1>");

            // ==== Form with SSID + Password ====
            client.println("<form action=\"/set\">");
            client.println("<p><input type=\"text\" name=\"ssid\" placeholder=\"Enter SSID\" value=\"" + ssid + "\"></p>");
            client.println("<p><input type=\"password\" name=\"password\" placeholder=\"Enter Password\" value=\"" + password + "\"></p>");
            client.println("<p><button type=\"submit\">Save</button></p>");
            client.println("</form>");

            client.println("<p>Current SSID: " + ssid + "</p>");
            client.println("<p>Current Password: " + password + "</p>");

            client.println("</body></html>");
            client.println();

            break;
          } else {
            currentLine = "";
          }

        } else if (c != '\r') {
          currentLine += c;
        }
      }
    }

    header = "";
    client.stop();
    Serial.println("Client disconnected.");
    Serial.println();
  }
}
