#!/usr/bin/env python3

import asyncio
import json
import time
import hashlib
import os
import sys
import signal
import argparse
import logging
import base64
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import websockets
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('pc_miner.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
DEFAULT_CONFIG = {
    "node_url": "ws://127.0.0.1:8080/ws",
    "reconnect_delay": 5,
    "max_reconnect_attempts": 10,
    "uptime_ping_interval": 30,
    "signing_window_ms": 2500,
    "max_level": 10,
    "level_stake_range": 1000,
}

# ==================== CRYPTOGRAPHY ====================
def generate_keypair() -> Tuple[str, str]:
    """Generate ECDSA secp256k1 keypair"""
    private_key = ec.generate_private_key(ec.SECP256K1())
    private_hex = private_key.private_numbers().private_value.to_bytes(32, 'big').hex()
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()
    return private_hex, public_pem

def sign_message(private_key_hex: str, message: str) -> str:
    """Sign a message using ECDSA secp256k1"""
    private_value = int(private_key_hex, 16)
    private_key = ec.derive_private_key(private_value, ec.SECP256K1())
    signature = private_key.sign(message.encode(), ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(signature)
    return r.to_bytes(32, 'big').hex() + s.to_bytes(32, 'big').hex()

def djb2_hash(data: str) -> str:
    """DJB2 hash for compatibility with AVR miners"""
    h = 5381
    for c in data:
        h = ((h << 5) + h) + ord(c)
    return format(h & 0xFFFFFFFF, '08x')

def generate_wallet() -> Tuple[str, str, str]:
    """Generate a wallet with ECDSA keypair"""
    priv_hex, pub_pem = generate_keypair()
    addr = "MCR_" + hashlib.sha256(pub_pem.encode()).hexdigest()[:32].upper()
    return addr, priv_hex, pub_pem

# ==================== DATA CLASSES ====================
@dataclass
class MinerState:
    """Miner state"""
    wallet: str = ""
    private_key: str = ""
    public_key: str = ""
    username: str = ""
    stake: int = 1000
    level: int = 1
    confirmed_balance: int = 0
    rewards: int = 0
    blocks_signed: int = 0
    uptime: int = 0
    today_uptime: int = 0
    registered: bool = False
    mining_enabled: bool = True
    is_validator: bool = False
    current_challenge: str = ""
    current_block_id: int = 0
    last_ping: float = 0
    last_reg_attempt: float = 0

@dataclass
class MinerStats:
    """Miner statistics"""
    start_time: float = field(default_factory=time.time)
    messages_sent: int = 0
    messages_received: int = 0
    challenges_signed: int = 0
    blocks_accepted: int = 0
    blocks_rejected: int = 0
    reconnections: int = 0
    errors: int = 0
    
    def get_uptime(self) -> str:
        uptime = int(time.time() - self.start_time)
        return f"{uptime//3600}h {(uptime%3600)//60}m {uptime%60}s"

# ==================== PC MINER ====================
class PCMiner:
    def __init__(self, username: str, wallet: str = "", private_key: str = "", public_key: str = ""):
        self.state = MinerState()
        self.stats = MinerStats()
        self.running = True
        self.websocket = None
        self.connected = False
        self.pending_messages = []
        
        # Set identity
        self.state.username = username
        
        if wallet and private_key and public_key:
            self.state.wallet = wallet
            self.state.private_key = private_key
            self.state.public_key = public_key
            logger.info(f"Using existing wallet: {wallet}")
        else:
            # Generate new wallet
            addr, priv, pub = generate_wallet()
            self.state.wallet = addr
            self.state.private_key = priv
            self.state.public_key = pub
            logger.info(f"Generated new wallet: {addr}")
            logger.info(f"Private key: {priv[:16]}... (keep this safe!)")
            
            # Save wallet to file
            self._save_wallet()
        
        # Load config
        self.node_url = DEFAULT_CONFIG["node_url"]
        self.reconnect_delay = DEFAULT_CONFIG["reconnect_delay"]
        self.max_reconnect_attempts = DEFAULT_CONFIG["max_reconnect_attempts"]
        self.uptime_ping_interval = DEFAULT_CONFIG["uptime_ping_interval"]
        self.signing_window_ms = DEFAULT_CONFIG["signing_window_ms"]
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"PC Miner initialized for {username}")
        logger.info(f"Wallet: {wallet}")
    
    def _signal_handler(self, signum, frame):
        logger.info("\n[SHUTDOWN] Stopping miner...")
        self.running = False
    
    def _save_wallet(self):
        """Save wallet to file"""
        wallet_file = f"pc_miner_wallet_{self.state.wallet[:16]}.json"
        wallet_data = {
            "username": self.state.username,
            "address": self.state.wallet,
            "private_key": self.state.private_key,
            "public_key": self.state.public_key,
            "created_at": time.time(),
            "version": "1.0"
        }
        try:
            with open(wallet_file, 'w') as f:
                json.dump(wallet_data, f, indent=2)
            logger.info(f"Wallet saved to: {wallet_file}")
        except Exception as e:
            logger.error(f"Failed to save wallet: {e}")
    
    # ==================== JSON BUILDERS ====================
    def build_register(self) -> dict:
        """Build registration message"""
        timestamp = int(time.time())
        msg = f"{self.state.username}{self.state.wallet}{timestamp}"
        signature = sign_message(self.state.private_key, msg)
        
        return {
            "type": "register",
            "validator_id": self.state.wallet,
            "public_key": self.state.public_key,
            "username": self.state.username,
            "wallet": self.state.wallet,
            "stake": self.state.stake,
            "level": self.state.level,
            "confirmed_balance": self.state.confirmed_balance,
            "rewards": self.state.rewards,
            "blocks": self.state.blocks_signed,
            "uptime": self.state.uptime,
            "today_uptime": self.state.today_uptime,
            "miner_type": "pc",
            "version": "1.0",
            "timestamp": timestamp,
            "signature": signature,
            "board": "PC"
        }
    
    def build_uptime_ping(self) -> dict:
        """Build uptime ping message"""
        return {
            "type": "uptime_ping",
            "validator_id": self.state.wallet,
            "username": self.state.username,
            "wallet": self.state.wallet,
            "uptime_seconds": self.state.uptime,
            "today_uptime": self.state.today_uptime,
            "stake": self.state.stake,
            "level": self.state.level,
            "confirmed_balance": self.state.confirmed_balance,
            "blocks_signed": self.state.blocks_signed,
            "rewards": self.state.rewards
        }
    
    def build_signature(self) -> dict:
        """Build block signature message"""
        message = f"{self.state.current_challenge}{self.state.wallet}{self.state.current_block_id}"
        signature = sign_message(self.state.private_key, message)
        
        return {
            "type": "block_signature",
            "validator_id": self.state.wallet,
            "username": self.state.username,
            "wallet": self.state.wallet,
            "challenge": self.state.current_challenge,
            "signature": signature,
            "block_id": self.state.current_block_id,
            "level": self.state.level,
            "stake": self.state.stake,
            "confirmed_balance": self.state.confirmed_balance,
            "blocks_signed": self.state.blocks_signed
        }
    
    # ==================== MESSAGE HANDLING ====================
    def process_message(self, data: dict):
        """Process messages from node"""
        msg_type = data.get("type", "unknown")
        
        if msg_type == "registered":
            self.state.registered = True
            logger.info("✅ Registration confirmed!")
            
            # Update level from node
            if "level" in data:
                new_level = data["level"]
                if new_level > self.state.level:
                    self.state.level = new_level
                    logger.info(f"Level updated to {self.state.level}")
            
            if "confirmed_balance" in data:
                self.state.confirmed_balance = data["confirmed_balance"]
            
            if "stake" in data:
                new_stake = data["stake"]
                if new_stake > self.state.stake:
                    self.state.stake = new_stake
                    logger.info(f"Stake updated to {self.state.stake}")
            
            return
        
        if msg_type == "challenge":
            if not self.state.mining_enabled:
                logger.debug("Mining disabled, skipping challenge")
                return
            
            self.state.current_challenge = data.get("challenge", "")
            self.state.current_block_id = data.get("block_id", 0)
            self.state.is_validator = True
            
            # Send signature
            sig_data = self.build_signature()
            self._send_message(sig_data)
            self.stats.challenges_signed += 1
            logger.info(f"Signed block {self.state.current_block_id}")
            
            # Set timeout for auto-slash
            asyncio.create_task(self._auto_slash_timer())
            return
        
        if msg_type == "block_accepted":
            self.state.is_validator = False
            self.stats.blocks_accepted += 1
            reward = data.get("reward", 0)
            if reward > 0:
                self.state.rewards += reward
                self.state.stake += reward
                self.state.confirmed_balance += reward
                self.state.blocks_signed += 1
                logger.info(f"✅ Block accepted! Reward: {reward} MCX")
            return
        
        if msg_type == "block_rejected":
            self.state.is_validator = False
            self.stats.blocks_rejected += 1
            reason = data.get("reason", "Unknown")
            logger.warning(f"❌ Block rejected: {reason}")
            return
        
        if msg_type == "ping":
            self._send_message({"type": "pong", "timestamp": time.time()})
            return
        
        if msg_type == "miner_control":
            action = data.get("action", "")
            if action == "stop":
                self.state.mining_enabled = False
                logger.info("⏹ Mining stopped by node")
            elif action == "start":
                self.state.mining_enabled = True
                logger.info("▶️ Mining started by node")
            elif action == "restart":
                self.state.mining_enabled = False
                asyncio.create_task(self._restart_miner())
            return
        
        if msg_type == "get_status":
            self._send_message(self.build_uptime_ping())
            return
        
        if msg_type == "level_update":
            if "stake" in data:
                self.state.stake = data["stake"]
                self._update_level()
            return
        
        if msg_type == "balance_update":
            if "confirmed_balance" in data:
                self.state.confirmed_balance = data["confirmed_balance"]
            return
    
    def _update_level(self):
        """Update level based on stake"""
        new_level = (self.state.stake - 1) // DEFAULT_CONFIG["level_stake_range"] + 1
        if new_level < 1:
            new_level = 1
        if new_level > DEFAULT_CONFIG["max_level"]:
            new_level = DEFAULT_CONFIG["max_level"]
        if new_level != self.state.level:
            self.state.level = new_level
            logger.info(f"Level updated to {self.state.level}")
    
    async def _auto_slash_timer(self):
        """Auto-slash if challenge not resolved"""
        await asyncio.sleep(self.signing_window_ms / 1000)
        if self.state.is_validator:
            self.state.is_validator = False
            logger.warning("⚠️ Missed signing window")
    
    async def _restart_miner(self):
        """Restart miner"""
        self.state.mining_enabled = False
        await asyncio.sleep(1)
        self.state.mining_enabled = True
        self._send_message(self.build_register())
        logger.info("🔄 Miner restarted")
    
    def _send_message(self, data: dict):
        """Send message to node"""
        if self.connected and self.websocket:
            try:
                asyncio.create_task(self.websocket.send(json.dumps(data)))
                self.stats.messages_sent += 1
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
                self.pending_messages.append(data)
        else:
            self.pending_messages.append(data)
            logger.debug("Not connected, buffered message")
    
    async def _flush_pending(self):
        """Send all pending messages"""
        if not self.connected or not self.websocket:
            return
        
        flushed = 0
        while self.pending_messages:
            try:
                msg = self.pending_messages.pop(0)
                await self.websocket.send(json.dumps(msg))
                self.stats.messages_sent += 1
                flushed += 1
            except Exception as e:
                logger.error(f"Failed to flush: {e}")
                self.pending_messages.append(msg)
                break
        
        if flushed > 0:
            logger.info(f"Flushed {flushed} pending messages")
    
    # ==================== WEBSOCKET ====================
    async def _connect_to_node(self):
        """Connect to node with auto-reconnect"""
        attempts = 0
        
        while self.running:
            try:
                logger.info(f"Connecting to {self.node_url}")
                
                async with websockets.connect(
                    self.node_url,
                    ping_interval=15,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,
                    open_timeout=10
                ) as ws:
                    self.websocket = ws
                    self.connected = True
                    attempts = 0
                    logger.info("✅ Connected to node")
                    
                    # Send registration
                    reg_data = self.build_register()
                    await ws.send(json.dumps(reg_data))
                    self.stats.messages_sent += 1
                    logger.info("📤 Registration sent")
                    
                    # Flush pending
                    await self._flush_pending()
                    
                    # Listen for messages
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            self.stats.messages_received += 1
                            self.process_message(data)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Invalid JSON: {e}")
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}")
            except Exception as e:
                logger.error(f"Connection error: {e}")
            
            self.connected = False
            self.websocket = None
            self.stats.reconnections += 1
            
            attempts += 1
            if attempts >= self.max_reconnect_attempts:
                logger.error("Max reconnection attempts reached")
                if not self.running:
                    break
                attempts = 0
            
            delay = min(self.reconnect_delay * (2 ** min(attempts, 5)), 60)
            logger.info(f"Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
    
    # ==================== LOOPS ====================
    async def _uptime_loop(self):
        """Update uptime and send ping"""
        while self.running:
            await asyncio.sleep(1)
            if self.state.registered:
                self.state.uptime += 1
                self.state.today_uptime += 1
            
            # Send uptime ping every interval
            if self.state.registered and self.state.uptime % self.uptime_ping_interval == 0:
                if self.state.uptime > 0:
                    ping_data = self.build_uptime_ping()
                    self._send_message(ping_data)
                    logger.debug(f"📤 Uptime ping: {self.state.uptime}s")
    
    async def _status_loop(self):
        """Print status periodically"""
        while self.running:
            await asyncio.sleep(30)
            
            if self.state.registered:
                logger.info(
                    f"[STATUS] Level: {self.state.level} | "
                    f"Stake: {self.state.stake} | "
                    f"Balance: {self.state.confirmed_balance} | "
                    f"Blocks: {self.state.blocks_signed} | "
                    f"Rewards: {self.state.rewards} | "
                    f"Uptime: {self.state.uptime}s"
                )
    
    async def _heartbeat_loop(self):
        """Send heartbeat ping"""
        while self.running:
            await asyncio.sleep(30)
            if self.connected:
                self._send_message({"type": "ping", "timestamp": time.time()})
    
    async def _re_register_loop(self):
        """Re-register if not registered"""
        while self.running:
            await asyncio.sleep(60)
            if not self.state.registered and self.connected:
                logger.info("Re-registering...")
                reg_data = self.build_register()
                self._send_message(reg_data)
    
    # ==================== RUN ====================
    async def run(self):
        """Run the miner"""
        print("\n" + "=" * 60)
        print("MICROCORE PC MINER v1.0")
        print("=" * 60)
        print(f"Username: {self.state.username}")
        print(f"Wallet: {self.state.wallet}")
        print(f"Node: {self.node_url}")
        print("=" * 60)
        print("\n🚀 Starting miner... Press Ctrl+C to stop\n")
        
        await asyncio.gather(
            self._connect_to_node(),
            self._uptime_loop(),
            self._status_loop(),
            self._heartbeat_loop(),
            self._re_register_loop(),
        )

# ==================== MAIN ====================
async def main():
    parser = argparse.ArgumentParser(description='MicroCore PC Miner')
    parser.add_argument('--username', type=str, required=True, help='Your username')
    parser.add_argument('--wallet', type=str, help='Your wallet address')
    parser.add_argument('--private-key', type=str, help='Your private key')
    parser.add_argument('--public-key', type=str, help='Your public key')
    parser.add_argument('--node', type=str, default=DEFAULT_CONFIG["node_url"], help='Node WebSocket URL')
    parser.add_argument('--stake', type=int, default=1000, help='Initial stake amount')
    args = parser.parse_args()
    
    # Update config
    DEFAULT_CONFIG["node_url"] = args.node
    
    miner = PCMiner(
        username=args.username,
        wallet=args.wallet or "",
        private_key=args.private_key or "",
        public_key=args.public_key or ""
    )
    miner.state.stake = args.stake
    miner._update_level()
    miner.node_url = args.node
    
    try:
        await miner.run()
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopped by user")
    finally:
        if miner.websocket:
            try:
                await miner.websocket.close()
            except:
                pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[EXIT] Goodbye!")
        sys.exit(0)
