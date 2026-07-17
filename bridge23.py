#!/usr/bin/env python3
"""
MICROCORE WIFI BRIDGE v23.0 — RAW SERIAL FORCE READ (FIXED)
================================================================================
FIXES:
- ✅ FORCE READ from serial port
- ✅ DEBUG: Print raw bytes received
- ✅ COM4 forced connection
- ✅ FIXED f-string syntax error
================================================================================
"""

import asyncio
import serial
import serial.tools.list_ports
import json
import websockets
import time
import os
import sys
import signal
import logging
import traceback
from collections import deque
from typing import Dict, List, Optional
from dataclasses import dataclass, field

# ==================== CONFIGURATION ====================
CONFIG = {
    "bootstrap_nodes": ["127.0.0.1:8080"],
    "ws_path": "/ws",
    "baud_rate": 115200,
    "serial_timeout": 1,
    "write_timeout": 1,
    "force_com4": True,
    "scan_interval": 5,
    "max_reconnect_attempts": 10,
    "reconnect_delay": 5,
    "heartbeat_interval": 30,
    "max_buffer_size": 1000,
}

PEER_CACHE_FILE = "bridge_peers.json"
LOG_FILE = "bridge.log"

FORWARD_TO_ARDUINO = [
    "challenge",
    "block_accepted",
    "block_rejected",
    "registered",
    "miner_control",
    "get_status",
    "ping",
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== DATA CLASSES ====================
@dataclass
class MinerInfo:
    port: str
    username: str = "unknown"
    validator_id: str = "unknown"
    wallet: str = ""
    board: str = "Unknown"
    level: int = 1
    stake: int = 1000
    confirmed_balance: int = 0
    rewards: int = 0
    blocks: int = 0
    uptime: int = 0
    today_uptime: int = 0
    last_seen: float = 0
    registered: bool = False
    active: bool = True
    eeprom_reset: bool = False
    
    def update_from_data(self, data: dict):
        level = data.get("level", 1)
        if isinstance(level, (int, float)) and 1 <= level <= 10:
            self.level = int(level)
        
        stake = data.get("stake", 1000)
        if isinstance(stake, (int, float)) and 100 <= stake <= 1000000:
            self.stake = int(stake)
        
        confirmed = data.get("confirmed_balance", 0)
        if isinstance(confirmed, (int, float)) and confirmed >= 0:
            self.confirmed_balance = int(confirmed)
        
        self.username = data.get("username", self.username)
        self.validator_id = data.get("validator_id", self.validator_id)
        self.wallet = data.get("wallet", self.wallet)
        self.board = data.get("board", self.board)
        self.rewards = max(0, data.get("rewards", self.rewards))
        self.blocks = max(0, data.get("blocks", self.blocks))
        self.uptime = max(0, data.get("uptime", self.uptime))
        self.today_uptime = max(0, data.get("today_uptime", self.today_uptime))
        self.last_seen = time.time()
        
        if data.get("type") in ["register", "miner_startup"]:
            self.registered = True
            self.active = True
    
    def to_dict(self) -> dict:
        return {
            "port": self.port,
            "username": self.username,
            "validator_id": self.validator_id,
            "wallet": self.wallet,
            "board": self.board,
            "level": self.level,
            "stake": self.stake,
            "confirmed_balance": self.confirmed_balance,
            "rewards": self.rewards,
            "blocks": self.blocks,
            "uptime": self.uptime,
            "today_uptime": self.today_uptime,
            "last_seen": self.last_seen,
            "registered": self.registered,
            "active": self.active,
            "eeprom_reset": self.eeprom_reset
        }

@dataclass
class BridgeStats:
    start_time: float = field(default_factory=time.time)
    messages_sent: int = 0
    messages_received: int = 0
    arduino_messages: int = 0
    invalid_json: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    reconnections: int = 0
    errors: int = 0
    registrations_forwarded: int = 0
    messages_filtered: int = 0
    messages_buffered: int = 0
    bridge_registered: bool = False
    
    def get_uptime(self) -> str:
        uptime = int(time.time() - self.start_time)
        return f"{uptime//3600}h {(uptime%3600)//60}m {uptime%60}s"

# ==================== PEER MANAGER ====================
class PeerManager:
    def __init__(self):
        self.peers: List[str] = []
        self.current_index: int = 0
        self.discovered: set = set()
        self._load_cache()
        self._init_from_config()
    
    def _init_from_config(self):
        for peer in CONFIG["bootstrap_nodes"]:
            self.add_peer(peer)
    
    def _load_cache(self):
        try:
            if os.path.exists(PEER_CACHE_FILE):
                with open(PEER_CACHE_FILE, 'r') as f:
                    cached = json.load(f)
                    for peer in cached:
                        self.add_peer(peer)
                logger.info(f"[PEERS] Loaded {len(cached)} peers from cache")
        except Exception as e:
            pass
    
    def _save_cache(self):
        try:
            with open(PEER_CACHE_FILE, 'w') as f:
                json.dump(list(self.discovered), f, indent=2)
        except Exception as e:
            pass
    
    def add_peer(self, peer: str):
        peer = peer.strip()
        if not peer:
            return
        
        if not peer.startswith("ws://") and not peer.startswith("wss://"):
            peer = f"ws://{peer}"
        
        if not peer.endswith("/ws"):
            if peer.endswith("/"):
                peer = f"{peer}ws"
            else:
                peer = f"{peer}/ws"
        
        if peer not in self.discovered:
            self.discovered.add(peer)
            self.peers.append(peer)
            self._save_cache()
            logger.info(f"[PEERS] Added: {peer}")
    
    def get_current_peer(self) -> Optional[str]:
        if not self.peers:
            return None
        return self.peers[self.current_index % len(self.peers)]
    
    def switch_peer(self):
        if not self.peers:
            return None
        self.current_index = (self.current_index + 1) % len(self.peers)
        return self.get_current_peer()

# ==================== SERIAL MANAGER ====================
class SerialManager:
    def __init__(self):
        self.ports: Dict[str, serial.Serial] = {}
        self.miners: Dict[str, MinerInfo] = {}
        self.buffers: Dict[str, deque] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
    
    def is_port_open(self, port: str) -> bool:
        return port in self.ports and self.ports[port].is_open
    
    async def open_port(self, port: str) -> bool:
        async with self._lock:
            if port in self.ports and self.ports[port].is_open:
                return True
            
            try:
                ser = serial.Serial(
                    port,
                    CONFIG["baud_rate"],
                    timeout=CONFIG["serial_timeout"],
                    write_timeout=CONFIG["write_timeout"],
                    exclusive=True
                )
                ser.reset_input_buffer()
                ser.reset_output_buffer()
                
                self.ports[port] = ser
                if port not in self.miners:
                    self.miners[port] = MinerInfo(port=port)
                
                logger.info(f"[SERIAL] ✅ Opened {port}")
                return True
                
            except serial.SerialException as e:
                if "Access is denied" in str(e):
                    logger.warning(f"[SERIAL] {port} in use by another program")
                else:
                    logger.error(f"[SERIAL] Failed to open {port}: {e}")
                return False
            except Exception as e:
                logger.error(f"[SERIAL] Unexpected error opening {port}: {e}")
                return False
    
    async def close_port(self, port: str):
        async with self._lock:
            if port in self.ports:
                try:
                    self.ports[port].close()
                except:
                    pass
                del self.ports[port]
            
            if port in self.tasks:
                self.tasks[port].cancel()
                del self.tasks[port]
            
            if port in self.miners:
                self.miners[port].active = False
            
            logger.info(f"[SERIAL] Closed {port}")
    
    async def write_to_port(self, port: str, data: str) -> bool:
        if not self.is_port_open(port):
            return False
        
        try:
            self.ports[port].write((data + "\n").encode())
            return True
        except Exception as e:
            logger.error(f"[SERIAL] Write to {port} failed: {e}")
            return False
    
    def get_all_ports(self) -> List[str]:
        return list(self.ports.keys())
    
    def get_miner_info(self, port: str) -> Optional[dict]:
        if port in self.miners:
            return self.miners[port].to_dict()
        return None
    
    def update_miner(self, port: str, data: dict):
        if port in self.miners:
            self.miners[port].update_from_data(data)
            return True
        return False

# ==================== PORT SCANNER ====================
class PortScanner:
    @staticmethod
    def find_all() -> List[str]:
        ports = serial.tools.list_ports.comports()
        found = []
        
        for port in ports:
            if "COM" in port.device or "tty" in port.device:
                exclude_keywords = ["Bluetooth", "BlueTooth", "BT", "Modem", "Printer"]
                exclude = False
                for keyword in exclude_keywords:
                    if keyword.lower() in port.description.lower():
                        exclude = True
                        break
                if not exclude:
                    found.append(port.device)
                    logger.debug(f"[SCAN] Found: {port.device}")
        
        return found

# ==================== WEBSOCKET BRIDGE ====================
class WebSocketBridge:
    def __init__(self):
        self.running = True
        self.websocket = None
        self.connected = False
        self.bridge_registered = False
        self.pending_messages: deque = deque()
        
        self.peer_manager = PeerManager()
        self.serial_manager = SerialManager()
        self.stats = BridgeStats()
        
        self.reconnect_attempts = 0
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        logger.info("\n[SHUTDOWN] Stopping bridge...")
        self.running = False
    
    # ==================== WEBSOCKET ====================
    async def _connect_to_node(self):
        while self.running:
            peer_url = self.peer_manager.get_current_peer()
            if not peer_url:
                logger.error("[NODE] No peers available")
                await asyncio.sleep(10)
                continue
            
            try:
                logger.info(f"[NODE] Connecting to {peer_url}")
                
                if not peer_url.startswith("ws://") and not peer_url.startswith("wss://"):
                    peer_url = f"ws://{peer_url}"
                
                if not peer_url.endswith("/ws"):
                    if peer_url.endswith("/"):
                        peer_url = f"{peer_url}ws"
                    else:
                        peer_url = f"{peer_url}/ws"
                
                logger.info(f"[NODE] Final URL: {peer_url}")
                
                async with websockets.connect(
                    peer_url,
                    ping_interval=15,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,
                    open_timeout=10
                ) as ws:
                    self.websocket = ws
                    self.connected = True
                    self.bridge_registered = False
                    self.reconnect_attempts = 0
                    logger.info(f"[NODE] ✅ WebSocket connected to {peer_url}")
                    
                    try:
                        await ws.send(json.dumps({
                            "type": "register_bridge",
                            "version": "23.0",
                            "timestamp": int(time.time())
                        }))
                        logger.info("[NODE] ✅ Sent bridge registration")
                    except Exception as e:
                        logger.error(f"[NODE] Registration send failed: {e}")
                    
                    if self.pending_messages:
                        logger.info(f"[NODE] 📤 Flushing {len(self.pending_messages)} pending messages")
                        flushed = 0
                        while self.pending_messages:
                            try:
                                msg = self.pending_messages.popleft()
                                await ws.send(msg)
                                self.stats.messages_sent += 1
                                flushed += 1
                            except Exception as e:
                                logger.error(f"[NODE] Failed to flush: {e}")
                                self.pending_messages.appendleft(msg)
                                break
                        logger.info(f"[NODE] ✅ Flushed {flushed} messages")
                    
                    async for message in ws:
                        await self._handle_node_message(message)
                        
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[NODE] Connection closed: {e}")
            except asyncio.TimeoutError:
                logger.warning("[NODE] Connection timeout")
            except Exception as e:
                logger.error(f"[NODE] Connection error: {e}")
            
            self.connected = False
            self.websocket = None
            self.bridge_registered = False
            self.stats.reconnections += 1
            
            if self.peer_manager.peers:
                self.peer_manager.switch_peer()
            
            delay = min(CONFIG["reconnect_delay"] * (2 ** min(self.reconnect_attempts, 5)), 60)
            logger.info(f"[NODE] Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            self.reconnect_attempts += 1
    
    async def _handle_node_message(self, message: str):
        try:
            data = json.loads(message)
            self.stats.messages_received += 1
            self.stats.bytes_received += len(message)
            
            msg_type = data.get("type", "unknown")
            logger.debug(f"[NODE] Received: {msg_type}")
            
            if msg_type == "registered":
                if "Bridge" in str(data.get("message", "")):
                    logger.info(f"[NODE] ✅ Bridge registration confirmed!")
                    self.bridge_registered = True
                else:
                    logger.info(f"[NODE] ✅ Miner registration confirmed!")
                    if self.serial_manager.get_all_ports():
                        for port in self.serial_manager.get_all_ports():
                            await self.serial_manager.write_to_port(port, message)
                return
            
            if msg_type == "peers":
                for peer in data.get("peers", []):
                    self.peer_manager.add_peer(peer)
                logger.info(f"[NODE] Got {len(data.get('peers', []))} peers")
                return
            
            if self.serial_manager.get_all_ports():
                if msg_type in FORWARD_TO_ARDUINO:
                    for port in self.serial_manager.get_all_ports():
                        await self.serial_manager.write_to_port(port, message)
                        self.stats.bytes_sent += len(message)
                    logger.debug(f"[NODE] Forwarded {msg_type} to Arduino")
                else:
                    self.stats.messages_filtered += 1
                
        except json.JSONDecodeError as e:
            logger.warning(f"[NODE] Invalid JSON: {message[:100]} - {e}")
        except Exception as e:
            logger.error(f"[NODE] Handler error: {e}")
    
    # ==================== SERIAL ====================
    async def _handle_serial_ports(self):
        while self.running:
            found_ports = PortScanner.find_all()
            
            if CONFIG["force_com4"] and "COM4" not in self.serial_manager.ports:
                logger.info("[SERIAL] 🔍 Forcing COM4...")
                if await self.serial_manager.open_port("COM4"):
                    task = asyncio.create_task(self._handle_serial_messages("COM4"))
                    self.serial_manager.tasks["COM4"] = task
            
            for port in found_ports:
                if port not in self.serial_manager.ports:
                    if await self.serial_manager.open_port(port):
                        task = asyncio.create_task(self._handle_serial_messages(port))
                        self.serial_manager.tasks[port] = task
            
            for port in list(self.serial_manager.ports.keys()):
                if not self.serial_manager.is_port_open(port):
                    logger.warning(f"[SERIAL] {port} closed, reopening...")
                    await self.serial_manager.close_port(port)
                    if await self.serial_manager.open_port(port):
                        task = asyncio.create_task(self._handle_serial_messages(port))
                        self.serial_manager.tasks[port] = task
            
            await asyncio.sleep(CONFIG["scan_interval"])
    
    # ==================== FIXED SERIAL HANDLER ====================
    async def _handle_serial_messages(self, port: str):
        if not self.serial_manager.is_port_open(port):
            return
        
        ser = self.serial_manager.ports[port]
        buffer = ""
        error_count = 0
        
        logger.info(f"[SERIAL:{port}] 👂 Listening...")
        
        while self.running and self.serial_manager.is_port_open(port):
            try:
                # ✅ FORCE READ - bypass in_waiting issues
                try:
                    # Read all available bytes
                    raw = ser.read(ser.in_waiting or 1)
                    if raw:
                        logger.info(f"[SERIAL:{port}] 📥 RAW BYTES: {raw.hex()}")
                        try:
                            text = raw.decode('utf-8', errors='replace')
                            logger.info(f"[SERIAL:{port}] 📥 DECODED: {repr(text)}")
                            buffer += text
                        except Exception as e:
                            logger.error(f"[SERIAL:{port}] Decode error: {e}")
                except serial.SerialException as e:
                    logger.error(f"[SERIAL:{port}] Read error: {e}")
                    await asyncio.sleep(1)
                    continue
                
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    self.stats.arduino_messages += 1
                    logger.info(f"[SERIAL:{port}] 📥 LINE: {line[:200]}")
                    
                    # Try to find JSON
                    start = line.find('{')
                    if start >= 0:
                        end = line.rfind('}')
                        if end > start:
                            json_line = line[start:end+1]
                            logger.info(f"[SERIAL:{port}] 📥 COMPLETE JSON: {json_line[:200]}")
                            
                            try:
                                data = json.loads(json_line)
                                msg_type = data.get("type", "unknown")
                                logger.info(f"[SERIAL:{port}] 📝 MSG TYPE: {msg_type}")
                                
                                if msg_type == "register":
                                    logger.info(f"[SERIAL:{port}] 👤 REGISTRATION from {data.get('username', 'unknown')}")
                                    if self.connected and self.websocket:
                                        try:
                                            await self.websocket.send(json_line)
                                            self.stats.messages_sent += 1
                                            self.stats.registrations_forwarded += 1
                                            logger.info(f"[SERIAL:{port}] ✅ FORWARDED TO NODE!")
                                        except Exception as e:
                                            logger.error(f"[SERIAL:{port}] Forward failed: {e}")
                                            self.pending_messages.append(json_line)
                                    else:
                                        self.pending_messages.append(json_line)
                                        logger.warning(f"[SERIAL:{port}] 📦 Buffered")
                                
                                elif msg_type == "diagnostic":
                                    logger.info(f"[SERIAL:{port}] 🔍 Diagnostic: {data.get('status', 'unknown')}")
                                
                                else:
                                    logger.info(f"[SERIAL:{port}] 📤 Unknown type: {msg_type}")
                                    if self.connected and self.websocket:
                                        try:
                                            await self.websocket.send(json_line)
                                            self.stats.messages_sent += 1
                                        except:
                                            pass
                                            
                            except json.JSONDecodeError as e:
                                logger.warning(f"[SERIAL:{port}] JSON error: {e}")
                        else:
                            logger.warning(f"[SERIAL:{port}] No closing brace found")
                    else:
                        logger.warning(f"[SERIAL:{port}] No opening brace found")
                
                if len(buffer) > 4096:
                    logger.warning(f"[SERIAL:{port}] Buffer overflow, resetting")
                    buffer = ""
                
                await asyncio.sleep(0.1)
                
            except serial.SerialException as e:
                error_count += 1
                if "Access is denied" in str(e):
                    logger.error(f"[SERIAL:{port}] Access denied")
                    await self.serial_manager.close_port(port)
                    break
                elif error_count > 5:
                    logger.error(f"[SERIAL:{port}] Too many errors ({error_count})")
                    await self.serial_manager.close_port(port)
                    break
                else:
                    logger.warning(f"[SERIAL:{port}] Serial error: {e}")
                    await asyncio.sleep(1)
                    
            except Exception as e:
                error_count += 1
                logger.error(f"[SERIAL:{port}] Handler error: {e}")
                if error_count > 10:
                    break
                await asyncio.sleep(1)
        
        logger.info(f"[SERIAL:{port}] 👂 Handler stopped")
    
    # ==================== HEARTBEAT ====================
    async def _heartbeat_loop(self):
        while self.running:
            await asyncio.sleep(CONFIG["heartbeat_interval"])
            
            if self.connected and self.websocket:
                try:
                    await self.websocket.send(json.dumps({
                        "type": "ping",
                        "timestamp": int(time.time())
                    }))
                except Exception as e:
                    logger.warning(f"[HEARTBEAT] Node ping failed: {e}")
            
            for port in self.serial_manager.get_all_ports():
                await self.serial_manager.write_to_port(port, 
                    f'{{"type":"heartbeat","timestamp":{int(time.time())}}}')
    
    # ==================== STATUS REPORTER ====================
    async def _status_reporter(self):
        while self.running:
            await asyncio.sleep(10)
            
            logger.info(f"\n{'='*60}")
            logger.info("[STATUS] BRIDGE v23.0 — RAW FORCE READ")
            logger.info(f"{'='*60}")
            logger.info(f"Uptime: {self.stats.get_uptime()}")
            logger.info(f"Arduino messages: {self.stats.arduino_messages}")
            logger.info(f"Invalid JSON: {self.stats.invalid_json}")
            logger.info(f"Messages to node: {self.stats.messages_sent}")
            logger.info(f"Messages from node: {self.stats.messages_received}")
            logger.info(f"Registrations forwarded: {self.stats.registrations_forwarded}")
            logger.info(f"Messages buffered: {self.stats.messages_buffered}")
            logger.info(f"Messages filtered: {self.stats.messages_filtered}")
            logger.info(f"Reconnections: {self.stats.reconnections}")
            logger.info(f"Pending buffer: {len(self.pending_messages)}")
            logger.info(f"Node: {'✅ Connected' if self.connected else '❌ Disconnected'}")
            logger.info(f"Bridge Registered: {'✅' if self.bridge_registered else '❌'}")
            logger.info(f"Active ports: {len(self.serial_manager.get_all_ports())}")
            
            for port in self.serial_manager.get_all_ports():
                info = self.serial_manager.get_miner_info(port)
                if info:
                    logger.info(f"  {port}: {info.get('username')} | Wallet: {info.get('wallet', 'unknown')[:16]}... | Lv{info.get('level')} | {info.get('stake')} MCX | Confirmed: {info.get('confirmed_balance', 0)}")
            
            logger.info(f"{'='*60}")
    
    # ==================== RUN ====================
    async def run(self):
        print("\n" + "=" * 60)
        print("MICROCORE WIFI BRIDGE v23.0 — RAW FORCE READ")
        print("=" * 60)
        print(f"Bootnodes: {CONFIG['bootstrap_nodes']}")
        print(f"WebSocket Path: {CONFIG['ws_path']}")
        print(f"Baud rate: {CONFIG['baud_rate']}")
        print(f"Force COM4: {CONFIG['force_com4']}")
        print("=" * 60)
        print("\n🚀 Starting bridge... Press Ctrl+C to stop\n")
        
        await asyncio.gather(
            self._connect_to_node(),
            self._handle_serial_ports(),
            self._heartbeat_loop(),
            self._status_reporter(),
        )

# ==================== MAIN ====================
async def main():
    bridge = WebSocketBridge()
    try:
        await bridge.run()
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopped by user")
    finally:
        for port in bridge.serial_manager.get_all_ports():
            await bridge.serial_manager.close_port(port)
        print("[SHUTDOWN] Goodbye!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[EXIT] Goodbye!")
        sys.exit(0)