// ==================== AVR FINAL — MINIMAL JSON ====================
#include <EEPROM.h>
#include <avr/wdt.h>
#include <avr/pgmspace.h>

#define SERIAL_BAUD 115200
#define JSON_BUF_SIZE 350
#define LED_PIN 13
#define LED_ON HIGH
#define LED_OFF LOW

// ==================== USER CONFIG ====================
const char WALLET[] PROGMEM = "MCR_6E678B5121FDB0FE35A2E1A09270916E";
const char PRIVATE_KEY[] PROGMEM = "MCR_6E678B5121FDB0FE35A2E1A09270916E";
const char USERNAME[] PROGMEM = "XAVER";

// ==================== CONSTANTS ====================
#define UPTIME_PING_INTERVAL 30000
#define RE_REGISTER_INTERVAL 60000
#define SIGNING_WINDOW_MS 2500
#define MAX_LEVEL 10
#define LEVEL_STAKE_RANGE 1000

// ==================== EEPROM ====================
#define EEPROM_STAKE_ADDR 0
#define EEPROM_REWARDS_ADDR 4
#define EEPROM_BLOCKS_ADDR 8
#define EEPROM_UPTIME_ADDR 12
#define EEPROM_TODAY_UPTIME_ADDR 16
#define EEPROM_LAST_RESET_ADDR 20
#define EEPROM_LEVEL_ADDR 24
#define EEPROM_CONFIRMED_BALANCE_ADDR 28
#define EEPROM_MAGIC_ADDR 32
#define MAGIC_NUMBER 0xA5A5A5A5

// ==================== GLOBAL VARIABLES ====================
char wallet[40];
char vid[40];
char username[20];
char challenge[33];
char jsonBuf[JSON_BUF_SIZE];

uint32_t stake = 1000;
uint32_t rewards = 0;
uint32_t blocksSigned = 0;
uint32_t uptime = 0;
uint32_t todayUptime = 0;
uint32_t lastReset = 0;
uint32_t level = 1;
uint32_t confirmedBalance = 0;

uint32_t lastPing = 0;
uint32_t lastRegAttempt = 0;
uint32_t lastChallenge = 0;
uint32_t blockId = 0;

uint8_t isRegistered = 0;
uint8_t isValidator = 0;
uint8_t miningEnabled = 1;
uint8_t isBanned = 0;

// ==================== LED ====================
void led_init() { pinMode(LED_PIN, OUTPUT); digitalWrite(LED_PIN, LOW); }
void led_on() { digitalWrite(LED_PIN, HIGH); }
void led_off() { digitalWrite(LED_PIN, LOW); }
void led_blink(int n, int d) { for(int i=0; i<n; i++) { led_on(); delay(d); led_off(); delay(d); } }

// ==================== DJB2 HASH ====================
void djb2_hash(const char* in, char* out) {
  uint32_t h = 5381;
  uint8_t i = 0;
  while (in[i] && i < 200) {
    h = ((h << 5) + h) + (uint8_t)in[i];
    i++;
  }
  sprintf(out, "%08lx", h);
}

// ==================== LEVEL ====================
void calcLevel() {
  level = (stake < LEVEL_STAKE_RANGE) ? 1 : ((stake - 1) / LEVEL_STAKE_RANGE) + 1;
  if (level < 1) level = 1;
  if (level > MAX_LEVEL) level = MAX_LEVEL;
}

// ==================== EEPROM ====================
void saveEEPROM() {
  EEPROM.put(EEPROM_STAKE_ADDR, stake);
  EEPROM.put(EEPROM_REWARDS_ADDR, rewards);
  EEPROM.put(EEPROM_BLOCKS_ADDR, blocksSigned);
  EEPROM.put(EEPROM_UPTIME_ADDR, uptime);
  EEPROM.put(EEPROM_TODAY_UPTIME_ADDR, todayUptime);
  EEPROM.put(EEPROM_LAST_RESET_ADDR, lastReset);
  EEPROM.put(EEPROM_LEVEL_ADDR, level);
  EEPROM.put(EEPROM_CONFIRMED_BALANCE_ADDR, confirmedBalance);
  EEPROM.put(EEPROM_MAGIC_ADDR, MAGIC_NUMBER);
}

void loadEEPROM() {
  uint32_t magic;
  EEPROM.get(EEPROM_MAGIC_ADDR, magic);
  if (magic == MAGIC_NUMBER) {
    EEPROM.get(EEPROM_STAKE_ADDR, stake);
    EEPROM.get(EEPROM_REWARDS_ADDR, rewards);
    EEPROM.get(EEPROM_BLOCKS_ADDR, blocksSigned);
    EEPROM.get(EEPROM_UPTIME_ADDR, uptime);
    EEPROM.get(EEPROM_TODAY_UPTIME_ADDR, todayUptime);
    EEPROM.get(EEPROM_LAST_RESET_ADDR, lastReset);
    EEPROM.get(EEPROM_LEVEL_ADDR, level);
    EEPROM.get(EEPROM_CONFIRMED_BALANCE_ADDR, confirmedBalance);
    calcLevel();
  } else {
    stake = 1000; rewards = 0; blocksSigned = 0;
    uptime = 0; todayUptime = 0; lastReset = millis() / 1000;
    level = 1; confirmedBalance = 0;
    saveEEPROM();
  }
}

// ==================== SEND JSON ====================
void sendJson(const char* buf) {
  if (buf[0] == '{') {
    Serial.println(buf);
    Serial.flush();
    delay(20);
  }
}

// ==================== BUILD REGISTRATION — MINIMAL! ====================
void buildRegister(char* buf) {
  char priv[40];
  char walletAddr[40];
  char sig[9];
  
  strcpy_P(walletAddr, WALLET);
  strcpy_P(priv, PRIVATE_KEY);
  strcpy_P(username, USERNAME);
  
  strcpy(wallet, walletAddr);
  strcpy(vid, walletAddr);
  
  calcLevel();
  uint32_t ts = millis() / 1000;
  
  char msg[150];
  snprintf(msg, sizeof(msg), "%s%s%lu", username, wallet, ts);
  char sigInput[200];
  snprintf(sigInput, sizeof(sigInput), "%s%s", priv, msg);
  djb2_hash(sigInput, sig);
  
  // ✅ MINIMAL JSON — ONLY ESSENTIAL FIELDS!
  snprintf_P(buf, JSON_BUF_SIZE,
    PSTR("{\"type\":\"register\",\"validator_id\":\"%s\",\"public_key\":\"%s\",\"username\":\"%s\",\"wallet\":\"%s\",\"stake\":%lu,\"level\":%lu,\"confirmed_balance\":0,\"miner_type\":\"avr\",\"timestamp\":%lu,\"signature\":\"%s\"}"),
    vid, priv, username, wallet, stake, level, ts, sig);
}

// ==================== BUILD UPTIME ====================
void buildUptime(char* buf) {
  calcLevel();
  snprintf_P(buf, JSON_BUF_SIZE,
    PSTR("{\"type\":\"uptime_ping\",\"validator_id\":\"%s\",\"username\":\"%s\",\"wallet\":\"%s\",\"stake\":%lu,\"level\":%lu,\"blocks_signed\":%lu}"),
    vid, username, wallet, stake, level, blocksSigned);
}

// ==================== BUILD SIGNATURE ====================
void buildSignature(char* buf) {
  char sig[9];
  char sigMsg[100];
  snprintf(sigMsg, sizeof(sigMsg), "%s%s%lu", challenge, vid, blockId);
  djb2_hash(sigMsg, sig);
  snprintf_P(buf, JSON_BUF_SIZE,
    PSTR("{\"type\":\"block_signature\",\"validator_id\":\"%s\",\"username\":\"%s\",\"wallet\":\"%s\",\"challenge\":\"%s\",\"signature\":\"%s\",\"block_id\":%lu,\"level\":%lu,\"stake\":%lu}"),
    vid, username, wallet, challenge, sig, blockId, level, stake);
}

// ==================== PROCESS MESSAGES ====================
void processMessage(const char* buf) {
  if (strstr_P(buf, PSTR("\"type\":\"registered\""))) {
    isRegistered = 1; isBanned = 0; led_blink(5, 50);
    return;
  }
  
  if (strstr_P(buf, PSTR("\"type\":\"challenge\""))) {
    if (!miningEnabled || isBanned) return;
    const char* p = strstr_P(buf, PSTR("\"challenge\":\""));
    if (p) { p += 12; uint8_t i = 0; while (*p && *p != '"' && i < 32) { challenge[i++] = *p++; } challenge[i] = 0; }
    p = strstr_P(buf, PSTR("\"block_id\":"));
    if (p) { p += 11; blockId = 0; while (*p >= '0' && *p <= '9') { blockId = blockId * 10 + (*p - '0'); p++; } }
    lastChallenge = millis(); isValidator = 1;
    char sigBuf[JSON_BUF_SIZE]; buildSignature(sigBuf); sendJson(sigBuf);
    return;
  }
  
  if (strstr_P(buf, PSTR("\"type\":\"block_accepted\""))) {
    uint32_t reward = 0;
    const char* p = strstr_P(buf, PSTR("\"reward\":"));
    if (p) { p += 8; while (*p >= '0' && *p <= '9') { reward = reward * 10 + (*p - '0'); p++; } }
    if (reward > 0) { rewards += reward; stake += reward; confirmedBalance += reward; blocksSigned++; calcLevel(); saveEEPROM(); led_blink(2, 50); }
    isValidator = 0;
    return;
  }
  
  if (strstr_P(buf, PSTR("\"type\":\"block_rejected\""))) { isValidator = 0; return; }
  
  if (strstr_P(buf, PSTR("\"type\":\"miner_control\""))) {
    const char* p = strstr_P(buf, PSTR("\"action\":\""));
    if (p) { p += 10;
      if (strncmp_P(p, PSTR("stop"), 3) == 0) { miningEnabled = 0; led_off(); }
      else if (strncmp_P(p, PSTR("start"), 4) == 0) { miningEnabled = 1; isBanned = 0; led_on(); char regBuf[JSON_BUF_SIZE]; buildRegister(regBuf); sendJson(regBuf); }
    }
    return;
  }
}

// ==================== READ SERIAL ====================
void readSerial() {
  while (Serial.available()) {
    char c = Serial.read();
    static uint16_t idx = 0;
    if (c == '\n' || c == '\r') {
      if (idx > 0) { jsonBuf[idx] = 0; processMessage(jsonBuf); idx = 0; }
    } else if (idx < JSON_BUF_SIZE - 1) { jsonBuf[idx++] = c; } else { idx = 0; }
  }
}

// ==================== SETUP ====================
void setup() {
  led_init();
  Serial.begin(SERIAL_BAUD);
  delay(3000);
  
  strcpy_P(username, USERNAME);
  strcpy_P(wallet, WALLET);
  strcpy(vid, wallet);
  
  loadEEPROM();
  calcLevel();
  
  led_blink(3, 100);
  
  // ✅ DIAGNOSTIC
  char diagBuf[JSON_BUF_SIZE];
  snprintf_P(diagBuf, JSON_BUF_SIZE,
    PSTR("{\"type\":\"diagnostic\",\"status\":\"ready\",\"level\":%lu,\"stake\":%lu,\"confirmed_balance\":%lu,\"wallet\":\"%s\"}"),
    level, stake, confirmedBalance, wallet);
  sendJson(diagBuf);
  
  delay(500);
  
  // ✅ REGISTRATION
  char regBuf[JSON_BUF_SIZE];
  buildRegister(regBuf);
  sendJson(regBuf);
  
  led_blink(5, 50);
}

// ==================== LOOP ====================
void loop() {
  readSerial();
  
  static uint32_t lastUptimeUpdate = 0;
  if (millis() - lastUptimeUpdate > 1000) {
    lastUptimeUpdate = millis();
    if (isRegistered) { uptime++; todayUptime++; saveEEPROM(); }
  }
  
  if (!isRegistered && !isBanned && millis() - lastRegAttempt > RE_REGISTER_INTERVAL) {
    char regBuf[JSON_BUF_SIZE];
    buildRegister(regBuf);
    sendJson(regBuf);
    lastRegAttempt = millis();
  }
  
  if (isRegistered && millis() - lastPing > UPTIME_PING_INTERVAL) {
    lastPing = millis();
    char upBuf[JSON_BUF_SIZE];
    buildUptime(upBuf);
    sendJson(upBuf);
    led_blink(1, 20);
  }
  
  if (isValidator && millis() - lastChallenge > SIGNING_WINDOW_MS) {
    isValidator = 0;
  }
  
  if (!isBanned && miningEnabled) {
    if (isValidator) { led_on(); delay(100); led_off(); delay(100); }
    else if (isRegistered) { led_on(); }
    else { led_blink(1, 200); delay(200); }
  } else { led_off(); }
  
  delay(10);
}
