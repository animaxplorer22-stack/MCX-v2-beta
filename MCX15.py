#!/usr/bin/env python3

import asyncio
import json
import time
import hashlib
import sqlite3
import random
import os
import sys
import socket
import struct
import secrets
import argparse
import traceback
import logging
import threading
import queue
import signal
import base64
import binascii
import re
import hmac
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple, Union, Any
from enum import Enum, auto
from datetime import datetime, timedelta
from functools import wraps, lru_cache
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse

# ==================== FORCE UTF-8 FOR WINDOWS ====================
import sys
import io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# ==================== DEPENDENCY CHECK ====================
def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        __import__(import_name)
        return True
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        __import__(import_name)
        return True

REQUIRED_PACKAGES = [
    ("websockets", "websockets"),
    ("requests", "requests"),
    ("cryptography", "cryptography"),
    ("aiohttp", "aiohttp"),
    ("pycryptodome", "Crypto"),
    ("python-dotenv", "dotenv"),
    ("web3", "web3"),
]

for pkg, import_name in REQUIRED_PACKAGES:
    install_and_import(pkg, import_name)

import websockets
from websockets import serve
import requests
import aiohttp
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from dotenv import load_dotenv
from web3 import Web3

import warnings
warnings.filterwarnings("ignore")

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        RotatingFileHandler('microcore.log', maxBytes=10_485_760, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
NODE_HOST = "127.0.0.1"
NODE_PORT = 8080
WS_PATH = "/ws"
P2P_PORT = 8081
HTTP_PORT = 8082

SYMBOL = "MCX"
NAME = "MicroCore"
VERSION = "15.0-REGISTRATION-FIXED"

# ==================== API KEYS ====================
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
POLSCAN_API_KEY = os.getenv("POLSCAN_API_KEY", "")
TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "")

# ==================== DUCO CONFIG ====================
DUCO_PAYMENT_ADDRESS = "XAVER_KENG_XUAN_YI"
DUCO_API_URL = "https://api.ducoin.io/v1"
DUCO_MEMO_SWAP = "swap"
DUCO_MEMO_LIQUIDITY = "liquidity"

# ==================== DECENTRALIZED MULTISIG BRIDGE ====================
BTC_MULTISIG_ADDRESS = "bc1qmultisig5of10xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
USDT_MULTISIG_ADDRESS = "0xmultisig5of10xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
POL_MULTISIG_ADDRESS = "0xmultisig5of10xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
TRX_MULTISIG_ADDRESS = "Tmultisig5of10xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

BRIDGE_ADDRESSES = {
    "BTC": BTC_MULTISIG_ADDRESS,
    "USDT": USDT_MULTISIG_ADDRESS,
    "POL": POL_MULTISIG_ADDRESS,
    "TRX": TRX_MULTISIG_ADDRESS,
}

MULTISIG_THRESHOLD = 5
MULTISIG_TOTAL_SIGNERS = 10
MULTISIG_SIGNERS = []

# ==================== TOKENOMICS ====================
INITIAL_BLOCK_REWARD = 18
HALVING_INTERVAL = 4_204_800
MAX_SUPPLY = 3_281_040_000
GENESIS_SUPPLY = 100_000

LEVEL_STAKE_RANGE = 1000
MAX_LEVEL = 10
MIN_WALLETS_FOR_NEXT_LEVEL = 10

LEVEL_BLOCK_INTERVALS = {
    1: 40, 2: 35, 3: 30, 4: 25, 5: 20,
    6: 15, 7: 10, 8: 9, 9: 8, 10: 7
}

LEVEL_CAPS = {
    1: 113_529_600, 2: 129_744_000, 3: 151_372_800, 4: 181_647_360, 5: 227_059_200,
    6: 302_745_600, 7: 454_118_400, 8: 504_576_000, 9: 567_648_000, 10: 648_740_160
}

# ==================== REWARD DISTRIBUTION ====================
REWARD_MINER_SHARE = 0.75
REWARD_UPTIME_SHARE = 0.20999
REWARD_NODE_SHARE = 0.03
REWARD_LP_SHARE = 0.01
REWARD_BUYER_SHARE = 0.00001

# ==================== FEE CONFIG ====================
SWAP_FEE_RATE = 0.003
LP_FEE_SHARE = 0.60
NODE_FEE_SHARE = 0.40

DEX_GAS_FEE_RATE = 0.006
DEX_GAS_FEE_MIN = 1
DEX_GAS_FEE_MAX = 100

# ==================== CONSENSUS ====================
SIGNING_WINDOW_MS = 2500
SLASH_RATE = 0.10
MIN_VALIDATORS_PER_BLOCK = 10
UPTIME_PING_INTERVAL = 30
DISTRIBUTION_INTERVAL_SEC = 300
BUYER_REWARD_INTERVAL_DAYS = 7

MAX_PEERS = 30
SYNC_INTERVAL = 10
HEARTBEAT_INTERVAL = 30
PEX_INTERVAL = 60
DISCOVERY_INTERVAL = 300

BAN_THRESHOLD = 5
BAN_DURATION = 3600
SLASH_COOLDOWN = 60

TRANSFER_FEE_RATE = 0.006
TRANSFER_FEE_MIN = 0.01
MEMPOOL_MAX_SIZE = 10000
MEMPOOL_EXPIRY = 3600

# ==================== BOOTSTRAP NODES ====================
BOOTSTRAP_NODES = []

PEER_CACHE_FILE = "microcore_peers.json"
NODE_CACHE_FILE = "microcore_nodes.json"
MINER_CACHE_FILE = "microcore_miners.json"

# ==================== P2P CONSTANTS ====================
P2P_MAGIC = b"MCR2"
P2P_VERSION = 2

(MSG_HANDSHAKE, MSG_PING, MSG_PONG, MSG_GET_BLOCKS, MSG_BLOCKS,
 MSG_NEW_BLOCK, MSG_NEW_TX, MSG_GET_PEERS, MSG_PEERS, MSG_SLASH,
 MSG_NODE_REGISTER, MSG_GET_STATUS, MSG_STATUS, MSG_GET_MEMPOOL,
 MSG_MEMPOOL, MSG_GET_MINERS, MSG_MINERS, MSG_BAN, MSG_UNBAN) = range(19)

# ==================== CACHE HELPERS ====================
def save_peers_to_cache(peers):
    try:
        unique = list(set(peers))
        with open(PEER_CACHE_FILE, 'w') as f:
            json.dump(unique, f, indent=2)
        logger.info(f"[CACHE] Saved {len(unique)} peers")
    except Exception as e:
        logger.error(f"[CACHE] Save peers failed: {e}")

def load_peers_from_cache():
    try:
        with open(PEER_CACHE_FILE, 'r') as f:
            peers = json.load(f)
        logger.info(f"[CACHE] Loaded {len(peers)} peers from cache")
        return peers
    except:
        logger.info("[CACHE] No peer cache file found")
        return []

def get_bootstrap_peers():
    peers = BOOTSTRAP_NODES.copy()
    cached = load_peers_from_cache()
    for p in cached:
        if p not in peers:
            peers.append(p)
    return peers

# ==================== CRYPTOGRAPHY ====================

def djb2_hash(data: str) -> str:
    """DJB2 hash - deterministic 8-char hex output for AVR miners"""
    h = 5381
    for c in data:
        h = ((h << 5) + h) + ord(c)
    return format(h & 0xFFFFFFFF, '08x')

def generate_keypair() -> tuple:
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

def verify_signature(public_key_pem: str, message: str, signature_hex: str, miner_type: str = "web") -> bool:
    """Verify signature based on miner type"""
    if miner_type in ["uno", "avr", "web", "esp32", "pico"]:
        expected = djb2_hash(f"{public_key_pem}{message}")
        result = signature_hex.lower() == expected.lower()
        if not result:
            logger.warning(f"[SIG] DJB2 mismatch! Expected: {expected}, Got: {signature_hex}")
        return result
    
    if miner_type in ["pc", "phone"]:
        try:
            public_key = serialization.load_pem_public_key(public_key_pem.encode())
            signature_bytes = bytes.fromhex(signature_hex)
            r = int.from_bytes(signature_bytes[:32], 'big')
            s = int.from_bytes(signature_bytes[32:], 'big')
            public_key.verify(encode_dss_signature(r, s), message.encode(), ec.ECDSA(hashes.SHA256()))
            return True
        except Exception as e:
            logger.debug(f"[SIG] ECDSA verification failed: {e}")
            return False
    
    return False

def sha256_hash(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()

def hash_block(b: dict) -> str:
    return hashlib.sha256(json.dumps(b, sort_keys=True).encode()).hexdigest()

def hash_transaction(tx: dict) -> str:
    return hashlib.sha256(json.dumps(tx, sort_keys=True).encode()).hexdigest()

def get_public_ip():
    try:
        return requests.get('https://api.ipify.org').json()['ip']
    except:
        try:
            return requests.get('https://icanhazip.com').text.strip()
        except:
            return None

def generate_wallet():
    """Generate a wallet with ECDSA keypair"""
    priv_hex, pub_pem = generate_keypair()
    addr = "MCR_" + hashlib.sha256(pub_pem.encode()).hexdigest()[:32].upper()
    return addr, priv_hex, pub_pem

def validate_wallet_address(wallet: str) -> bool:
    """Validate wallet address: MCR_ + 32 hex chars = 36 chars total"""
    if not wallet or not isinstance(wallet, str):
        return False
    if not wallet.startswith("MCR_"):
        return False
    if len(wallet) != 36:
        return False
    try:
        int(wallet[4:], 16)
        return True
    except ValueError:
        return False

# ==================== ENCRYPTION ====================
def encrypt_private_key(priv_hex: str, password: str) -> dict:
    salt = os.urandom(16)
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000)
    key = kdf.derive(password.encode())
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    encryptor = cipher.encryptor()
    padded = pad(priv_hex.encode(), AES.block_size)
    encrypted = encryptor.update(padded) + encryptor.finalize()
    return {
        "salt": base64.b64encode(salt).decode(),
        "iv": base64.b64encode(iv).decode(),
        "data": base64.b64encode(encrypted).decode()
    }

def decrypt_private_key(encrypted: dict, password: str) -> str:
    salt = base64.b64decode(encrypted["salt"])
    iv = base64.b64decode(encrypted["iv"])
    data = base64.b64decode(encrypted["data"])
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000)
    key = kdf.derive(password.encode())
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(data) + decryptor.finalize()
    return unpad(decrypted, AES.block_size).decode()

# ==================== P2P HELPERS ====================
def encode_p2p(msg_type: int, payload: dict) -> bytes:
    j = json.dumps(payload).encode()
    return P2P_MAGIC + struct.pack(">BBI", P2P_VERSION, msg_type, len(j)) + j

def decode_p2p(data: bytes) -> tuple:
    if len(data) < 10 or data[:4] != P2P_MAGIC:
        return None, None
    version = data[4]
    msg_type = data[5]
    length = struct.unpack(">I", data[6:10])[0]
    payload = json.loads(data[10:10+length].decode())
    return msg_type, payload

# ==================== FEE HELPERS ====================
def calculate_dex_gas_fee(amount: int) -> int:
    fee = int(amount * DEX_GAS_FEE_RATE)
    if fee < DEX_GAS_FEE_MIN:
        return DEX_GAS_FEE_MIN
    if fee > DEX_GAS_FEE_MAX:
        return DEX_GAS_FEE_MAX
    return fee

def calculate_transfer_fee(amount: float) -> float:
    fee = amount * TRANSFER_FEE_RATE
    return max(TRANSFER_FEE_MIN, fee)

def get_halving_reward(level: int, block_height: int) -> int:
    halvings = block_height // HALVING_INTERVAL
    reward = INITIAL_BLOCK_REWARD // (2 ** halvings)
    return max(reward, 1)

# ==================== RATE LIMITER ====================
class RateLimiter:
    def __init__(self, max_requests: int = 60, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self._requests: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task = None
        self._stats = defaultdict(int)
    
    async def start_cleanup(self):
        async def cleanup_loop():
            while True:
                await asyncio.sleep(60)
                await self.cleanup()
        self._cleanup_task = asyncio.create_task(cleanup_loop())
    
    async def cleanup(self):
        async with self._lock:
            now = time.time()
            for ip in list(self._requests.keys()):
                while self._requests[ip] and now - self._requests[ip][0] > self.window:
                    self._requests[ip].popleft()
                if not self._requests[ip]:
                    del self._requests[ip]
    
    async def is_allowed(self, client_ip: str) -> bool:
        async with self._lock:
            now = time.time()
            if client_ip not in self._requests:
                self._requests[client_ip] = deque()
            while self._requests[client_ip] and now - self._requests[client_ip][0] > self.window:
                self._requests[client_ip].popleft()
            if len(self._requests[client_ip]) >= self.max_requests:
                self._stats[client_ip] += 1
                return False
            self._requests[client_ip].append(now)
            return True
    
    def get_stats(self) -> dict:
        return {
            "active_ips": len(self._requests),
            "violations": dict(self._stats),
            "total_requests": sum(len(q) for q in self._requests.values())
        }

# ==================== HEALTH CHECKER ====================
class HealthChecker:
    def __init__(self):
        self.start_time = time.time()
        self.last_block_time = time.time()
        self.blocks_produced = 0
        self.peer_count = 0
        self.miner_count = 0
        self.errors = 0
        self.status = "starting"
        self._error_history = deque(maxlen=100)
        self._block_history = deque(maxlen=100)
    
    def get_status(self) -> dict:
        uptime = int(time.time() - self.start_time)
        block_rate = len(self._block_history) / max(uptime / 3600, 0.1)
        return {
            "status": self.status,
            "uptime": uptime,
            "uptime_hours": uptime / 3600,
            "blocks_produced": self.blocks_produced,
            "block_rate_per_hour": block_rate,
            "peers": self.peer_count,
            "miners": self.miner_count,
            "errors": self.errors,
            "error_rate": self.errors / max(uptime / 3600, 0.1),
            "last_block": int(self.last_block_time),
            "last_block_age": int(time.time() - self.last_block_time),
            "version": VERSION,
            "healthy": self.errors < 10 and (time.time() - self.last_block_time) < 300
        }
    
    def record_block(self, block_id: int):
        self.blocks_produced += 1
        self.last_block_time = time.time()
        self._block_history.append((block_id, time.time()))
    
    def record_error(self, error: str):
        self.errors += 1
        self._error_history.append((error, time.time()))
        if self.errors > 100:
            self.status = "degraded"
    
    def get_block_history(self, limit: int = 10) -> List[dict]:
        return [{"id": bid, "timestamp": ts} for bid, ts in list(self._block_history)[-limit:]]
    
# ==================== DATA STRUCTURES ====================
@dataclass
class Miner:
    vid: str
    pub: str
    username: str
    wallet: str
    stake: int
    level: int
    uptime: int
    today_uptime: int
    last_ping: float
    active: bool
    rewards: int
    blocks: int
    slashes: int
    misses: int
    mtype: str
    liquidity_provided: int = 0
    fees_collected: int = 0
    banned_until: float = 0
    created_at: float = field(default_factory=time.time)
    last_block: int = 0
    consecutive_misses: int = 0
    total_uptime: int = 0
    best_level: int = 1
    towers: List[int] = field(default_factory=list)
    permanent_towers: bool = False
    ip_address: str = ""
    node_id: str = ""
    version: str = VERSION
    confirmed_balance: int = 0
    password_hash: str = ""
    private_key_encrypted: str = ""

@dataclass
class Node:
    node_id: str
    username: str
    wallet: str
    ip: str
    port: int
    last_seen: float
    height: int
    active: bool
    rewards_earned: int
    version: str = VERSION
    peers: List[str] = field(default_factory=list)
    mining_enabled: bool = True
    staking_enabled: bool = True
    last_sync: float = 0
    is_multisig_signer: bool = False

@dataclass
class Transaction:
    tx_hash: str
    from_wallet: str
    to_wallet: str
    amount: int
    fee: int
    timestamp: float
    block_id: int
    status: str
    tx_type: str
    signature: str = ""
    nonce: int = 0
    confirmations: int = 0
    data: dict = field(default_factory=dict)
    confirmed: bool = False

@dataclass
class Block:
    id: int
    ts: float
    prev: str
    validators: List[str]
    level: int
    sigs: Dict[str, str]
    hash: str
    reward: int
    challenge: str = ""
    tx_count: int = 0
    transactions: List[str] = field(default_factory=list)
    merkle_root: str = ""
    nonce: int = 0
    difficulty: int = 1
    version: str = VERSION
    timestamp: float = field(default_factory=time.time)
    height: int = 0

@dataclass
class SwapRequest:
    request_id: str
    user_wallet: str
    token_from: str
    token_to: str
    amount_from: int
    amount_to: int
    status: str
    created_at: float
    completed_at: float = 0
    tx_hash: str = ""
    real_tx_hash: str = ""

@dataclass
class MultisigProposal:
    proposal_id: str
    token_symbol: str
    action: str
    amount: float
    recipient_wallet: str
    signatures: List[str] = field(default_factory=list)
    status: str = "pending"
    created_at: float = field(default_factory=time.time)
    tx_hash: str = ""
    memo: str = ""

@dataclass
class DucoPayment:
    wallet: str
    username: str
    txid: str
    amount: float
    memo: str
    timestamp: float
    verified: bool = False
    mcx_credited: int = 0

# ==================== MEMPOOL ====================
class Mempool:
    def __init__(self, max_size: int = MEMPOOL_MAX_SIZE):
        self.transactions: Dict[str, Transaction] = {}
        self.max_size = max_size
        self._lock = asyncio.Lock()
        self._expiry = MEMPOOL_EXPIRY
        self._pending_by_wallet: Dict[str, List[str]] = defaultdict(list)
        self._fees_by_amount: Dict[int, List[str]] = defaultdict(list)
    
    async def add_transaction(self, tx: Transaction) -> bool:
        async with self._lock:
            if len(self.transactions) >= self.max_size:
                await self._cleanup()
                if len(self.transactions) >= self.max_size:
                    return False
            if tx.tx_hash in self.transactions:
                return False
            self.transactions[tx.tx_hash] = tx
            self._pending_by_wallet[tx.from_wallet].append(tx.tx_hash)
            self._fees_by_amount[tx.fee].append(tx.tx_hash)
            return True
    
    async def remove_transaction(self, tx_hash: str) -> bool:
        async with self._lock:
            if tx_hash not in self.transactions:
                return False
            tx = self.transactions[tx_hash]
            self._pending_by_wallet[tx.from_wallet].remove(tx_hash)
            self._fees_by_amount[tx.fee].remove(tx_hash)
            del self.transactions[tx_hash]
            return True
    
    async def get_transactions(self, limit: int = 100) -> List[Transaction]:
        async with self._lock:
            await self._cleanup()
            sorted_txs = sorted(self.transactions.values(), key=lambda x: (x.fee, -x.timestamp), reverse=True)
            return sorted_txs[:limit]
    
    async def _cleanup(self):
        now = time.time()
        to_remove = []
        for tx_hash, tx in self.transactions.items():
            if now - tx.timestamp > self._expiry:
                to_remove.append(tx_hash)
        for tx_hash in to_remove:
            await self.remove_transaction(tx_hash)
        if to_remove:
            logger.info(f"[MEMPOOL] Removed {len(to_remove)} expired transactions")
    
    def size(self) -> int:
        return len(self.transactions)
    
    def get_stats(self) -> dict:
        return {
            "size": len(self.transactions),
            "max_size": self.max_size,
            "pending_wallets": len(self._pending_by_wallet),
            "avg_fee": sum(self.transactions[t].fee for t in self.transactions) / max(len(self.transactions), 1)
        }

# ==================== DECENTRALIZED MULTISIG BRIDGE ====================
class MultisigBridge:
    def __init__(self, net):
        self.net = net
        self.tokens = {}
        self.pending_swaps: Dict[str, SwapRequest] = {}
        self.multisig_proposals: Dict[str, MultisigProposal] = {}
        self.duco_payments: Dict[str, DucoPayment] = {}
        self._lock = asyncio.Lock()
        self.threshold = MULTISIG_THRESHOLD
        self.total_signers = MULTISIG_TOTAL_SIGNERS
        self.duco_pool_balance = 0
        
        self._load_multisig_state()
        self._init_bridge_addresses()
    
    def _load_multisig_state(self):
        try:
            c = self.net.conn.cursor()
            c.execute("SELECT node_id FROM nodes WHERE is_multisig_signer = 1 AND active = 1")
            rows = c.fetchall()
            for row in rows:
                if row[0] not in MULTISIG_SIGNERS:
                    MULTISIG_SIGNERS.append(row[0])
            
            c.execute("SELECT SUM(amount) FROM duco_pool WHERE action='liquidity'")
            row = c.fetchone()
            if row and row[0]:
                self.duco_pool_balance = row[0]
            
            logger.info(f"[MULTISIG] Loaded {len(MULTISIG_SIGNERS)} signers, DUCO pool: {self.duco_pool_balance}")
        except Exception as e:
            logger.warning(f"[MULTISIG] No signers found: {e}")
    
    def _init_bridge_addresses(self):
        c = self.net.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS real_token_bridges (
            token_symbol TEXT PRIMARY KEY,
            bridge_address TEXT NOT NULL,
            balance REAL DEFAULT 0,
            last_updated REAL DEFAULT 0,
            private_key_encrypted TEXT DEFAULT '',
            multisig_threshold INTEGER DEFAULT 5,
            total_signers INTEGER DEFAULT 10
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS bridge_swap_requests (
            request_id TEXT PRIMARY KEY,
            user_wallet TEXT NOT NULL,
            token_from TEXT NOT NULL,
            token_to TEXT NOT NULL,
            amount_from REAL NOT NULL,
            amount_to REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at REAL NOT NULL,
            completed_at REAL DEFAULT 0,
            tx_hash TEXT DEFAULT '',
            real_tx_hash TEXT DEFAULT '',
            error TEXT DEFAULT ''
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS multisig_proposals (
            proposal_id TEXT PRIMARY KEY,
            token_symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            amount REAL NOT NULL,
            recipient_wallet TEXT NOT NULL,
            signatures TEXT DEFAULT '[]',
            status TEXT DEFAULT 'pending',
            created_at REAL NOT NULL,
            tx_hash TEXT DEFAULT '',
            memo TEXT DEFAULT ''
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS duco_payments (
            txid TEXT PRIMARY KEY,
            wallet TEXT NOT NULL,
            username TEXT NOT NULL,
            amount REAL NOT NULL,
            memo TEXT NOT NULL,
            timestamp REAL NOT NULL,
            verified INTEGER DEFAULT 0,
            mcx_credited INTEGER DEFAULT 0
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS duco_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT NOT NULL,
            amount REAL NOT NULL,
            action TEXT NOT NULL,
            timestamp REAL NOT NULL,
            tx_hash TEXT DEFAULT ''
        )''')
        self.net.conn.commit()
        
        for symbol, address in BRIDGE_ADDRESSES.items():
            if symbol not in self.tokens:
                c.execute("INSERT OR REPLACE INTO real_token_bridges (token_symbol, bridge_address, balance, last_updated, multisig_threshold, total_signers) VALUES (?, ?, ?, ?, ?, ?)",
                         (symbol, address, 0, time.time(), MULTISIG_THRESHOLD, MULTISIG_TOTAL_SIGNERS))
                self.net.conn.commit()
                self.tokens[symbol] = {
                    "bridge_address": address,
                    "balance": 0,
                    "last_updated": time.time(),
                    "multisig_threshold": MULTISIG_THRESHOLD,
                    "total_signers": MULTISIG_TOTAL_SIGNERS
                }
                logger.info(f"[BRIDGE] Initialized MULTISIG {symbol} bridge at {address} (Balance: 0)")
    
    def get_multisig_signers(self) -> List[str]:
        signers = []
        for node in self.net.nodes.values():
            if node.active and node.is_multisig_signer:
                signers.append(node.node_id)
        return signers
    
    def can_approve_multisig(self, node_id: str) -> bool:
        if node_id in self.net.nodes:
            return self.net.nodes[node_id].active and self.net.nodes[node_id].is_multisig_signer
        return False
    
    async def create_multisig_proposal(self, token_symbol: str, action: str, amount: float, 
                                       recipient_wallet: str, proposer_node: str, memo: str = "") -> dict:
        async with self._lock:
            if token_symbol not in self.tokens:
                return {"success": False, "error": f"Token {token_symbol} not supported"}
            
            if not self.can_approve_multisig(proposer_node):
                return {"success": False, "error": "Proposer is not an authorized multisig signer"}
            
            proposal_id = f"{token_symbol}_{action}_{int(time.time())}_{secrets.token_hex(4)}"
            
            proposal = MultisigProposal(
                proposal_id=proposal_id,
                token_symbol=token_symbol,
                action=action,
                amount=amount,
                recipient_wallet=recipient_wallet,
                signatures=[proposer_node],
                status="pending",
                created_at=time.time(),
                memo=memo
            )
            
            self.multisig_proposals[proposal_id] = proposal
            self._save_multisig_proposal(proposal)
            
            return {
                "success": True,
                "proposal_id": proposal_id,
                "token_symbol": token_symbol,
                "action": action,
                "amount": amount,
                "recipient_wallet": recipient_wallet,
                "signatures": proposal.signatures,
                "threshold": self.threshold,
                "needed": self.threshold - len(proposal.signatures),
                "status": "pending",
                "memo": memo
            }
    
    async def approve_multisig_proposal(self, proposal_id: str, node_id: str) -> dict:
        async with self._lock:
            if proposal_id not in self.multisig_proposals:
                return {"success": False, "error": "Proposal not found"}
            
            proposal = self.multisig_proposals[proposal_id]
            
            if proposal.status != "pending":
                return {"success": False, "error": f"Proposal already {proposal.status}"}
            
            if not self.can_approve_multisig(node_id):
                return {"success": False, "error": "Node is not an authorized multisig signer"}
            
            if node_id in proposal.signatures:
                return {"success": False, "error": "Node already signed this proposal"}
            
            proposal.signatures.append(node_id)
            self._save_multisig_proposal(proposal)
            
            if len(proposal.signatures) >= self.threshold:
                proposal.status = "approved"
                self._save_multisig_proposal(proposal)
                result = await self._execute_multisig_proposal(proposal)
                if result["success"]:
                    return {
                        "success": True,
                        "proposal_id": proposal_id,
                        "status": "approved",
                        "executed": True,
                        "tx_hash": result.get("tx_hash", ""),
                        "message": "Proposal approved and executed"
                    }
                else:
                    return {
                        "success": False,
                        "proposal_id": proposal_id,
                        "status": "approved",
                        "executed": False,
                        "error": result.get("error", "Execution failed")
                    }
            
            return {
                "success": True,
                "proposal_id": proposal_id,
                "signatures": proposal.signatures,
                "threshold": self.threshold,
                "needed": self.threshold - len(proposal.signatures),
                "status": proposal.status
            }
    
    async def _execute_multisig_proposal(self, proposal: MultisigProposal) -> dict:
        token_symbol = proposal.token_symbol
        action = proposal.action
        amount = proposal.amount
        recipient = proposal.recipient_wallet
        memo = proposal.memo
        
        if token_symbol not in self.tokens:
            return {"success": False, "error": f"Token {token_symbol} not supported"}
        
        bridge = self.tokens[token_symbol]
        
        if action == "withdraw":
            if bridge["balance"] < amount:
                return {"success": False, "error": f"Insufficient {token_symbol} balance"}
            
            bridge["balance"] -= amount
            self._update_bridge_balance(token_symbol, bridge["balance"])
            
            tx_hash = hashlib.sha256(f"{proposal.proposal_id}{time.time()}".encode()).hexdigest()[:16]
            proposal.tx_hash = tx_hash
            
            return {"success": True, "tx_hash": tx_hash}
        
        elif action == "swap":
            if token_symbol == "DUCO":
                if not memo or not memo.startswith(DUCO_MEMO_SWAP):
                    return {"success": False, "error": "Invalid DUCO memo for swap"}
                
                mcx_price = self.net.dex.get_token_price("MCX", "DUCO")
                if mcx_price <= 0:
                    return {"success": False, "error": "Cannot determine MCX price from DEX"}
                
                mcx_amount = int(amount * mcx_price)
                
                if self.duco_pool_balance < amount:
                    return {"success": False, "error": f"Insufficient DUCO pool balance. Available: {self.duco_pool_balance}"}
                
                self.duco_pool_balance -= amount
                self.net.balances[recipient] = self.net.balances.get(recipient, 0) + mcx_amount
                self.net._save_balance(recipient, self.net.balances[recipient])
                
                return {"success": True, "message": "Swap completed", "mcx_amount": mcx_amount, "price": mcx_price}
            else:
                return {"success": True, "message": "Swap proposal approved"}
        
        elif action == "liquidity":
            if token_symbol == "DUCO":
                if not memo or not memo.startswith(DUCO_MEMO_LIQUIDITY):
                    return {"success": False, "error": "Invalid DUCO memo for liquidity"}
                
                self.duco_pool_balance += amount
                return {"success": True, "message": "Liquidity added"}
        
        return {"success": False, "error": f"Unknown action: {action}"}
    
    # ==================== PAYMENT VERIFICATION ====================
    async def verify_btc_payment(self, sender_address: str, expected_amount: float, txid: str = None) -> dict:
        if not txid:
            return {"success": False, "error": "Transaction ID required"}
        
        try:
            response = requests.get(f"https://blockchain.info/tx/{txid}?format=json", timeout=10)
            if response.status_code != 200:
                return {"success": False, "error": "Transaction not found"}
            
            tx_data = response.json()
            bridge_address = BTC_MULTISIG_ADDRESS
            total_received = 0
            for output in tx_data.get('out', []):
                if output.get('addr', '').lower() == bridge_address.lower():
                    total_received += output.get('value', 0) / 1e8
            
            if total_received >= expected_amount:
                self.tokens["BTC"]["balance"] += total_received
                self._update_bridge_balance("BTC", self.tokens["BTC"]["balance"])
                return {"success": True, "received": total_received, "txid": txid}
            
            return {"success": False, "error": f"Insufficient payment"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def verify_usdt_payment(self, sender_address: str, expected_amount: float, txid: str = None) -> dict:
        if not txid:
            return {"success": False, "error": "Transaction ID required"}
        
        try:
            USDT_CONTRACT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
            
            response = requests.get(
                f"https://api.etherscan.io/api?module=account&action=tokentx&contractaddress={USDT_CONTRACT}&address={sender_address}&sort=desc&apikey={ETHERSCAN_API_KEY}",
                timeout=10
            )
            
            if response.status_code != 200:
                return {"success": False, "error": "Transaction not found"}
            
            data = response.json()
            if data.get('status') != '1':
                return {"success": False, "error": "Failed to get transaction list"}
            
            bridge_address = USDT_MULTISIG_ADDRESS.lower()
            total_sent = 0
            for tx in data.get('result', []):
                if tx.get('hash', '').lower() == txid.lower():
                    if tx.get('to', '').lower() == bridge_address:
                        total_sent += int(tx.get('value', 0)) / 1e6
                    break
            
            if total_sent >= expected_amount:
                self.tokens["USDT"]["balance"] += total_sent
                self._update_bridge_balance("USDT", self.tokens["USDT"]["balance"])
                return {"success": True, "received": total_sent, "txid": txid}
            
            return {"success": False, "error": f"Insufficient payment"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def verify_pol_payment(self, sender_address: str, expected_amount: float, txid: str = None) -> dict:
        if not txid:
            return {"success": False, "error": "Transaction ID required"}
        
        try:
            POL_CONTRACT = "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0"
            
            response = requests.get(
                f"https://api.polygonscan.com/api?module=account&action=tokentx&contractaddress={POL_CONTRACT}&address={sender_address}&sort=desc&apikey={POLSCAN_API_KEY}",
                timeout=10
            )
            
            if response.status_code != 200:
                return {"success": False, "error": "Transaction not found"}
            
            data = response.json()
            if data.get('status') != '1':
                return {"success": False, "error": "Failed to get transaction list"}
            
            bridge_address = POL_MULTISIG_ADDRESS.lower()
            total_sent = 0
            for tx in data.get('result', []):
                if tx.get('hash', '').lower() == txid.lower():
                    if tx.get('to', '').lower() == bridge_address:
                        total_sent += int(tx.get('value', 0)) / 1e18
                    break
            
            if total_sent >= expected_amount:
                self.tokens["POL"]["balance"] += total_sent
                self._update_bridge_balance("POL", self.tokens["POL"]["balance"])
                return {"success": True, "received": total_sent, "txid": txid}
            
            return {"success": False, "error": f"Insufficient payment"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def verify_trx_payment(self, sender_address: str, expected_amount: float, txid: str = None) -> dict:
        if not txid:
            return {"success": False, "error": "Transaction ID required"}
        
        try:
            headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}
            response = requests.get(f"https://api.trongrid.io/v1/transactions/{txid}", headers=headers, timeout=10)
            if response.status_code != 200:
                return {"success": False, "error": "Transaction not found"}
            
            data = response.json()
            
            bridge_address = TRX_MULTISIG_ADDRESS
            total_sent = 0
            
            if data.get('to', '').lower() == bridge_address.lower():
                total_sent += int(data.get('amount', 0)) / 1e6
            
            if total_sent >= expected_amount:
                self.tokens["TRX"]["balance"] += total_sent
                self._update_bridge_balance("TRX", self.tokens["TRX"]["balance"])
                return {"success": True, "received": total_sent, "txid": txid}
            
            return {"success": False, "error": f"Insufficient payment"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def verify_duco_payment(self, sender_address: str, expected_amount: float, txid: str = None, 
                                   memo: str = None, tx_time: float = None) -> dict:
        if not txid:
            return {"success": False, "error": "Transaction ID required"}
        
        try:
            if txid and memo:
                mcx_price = self.net.dex.get_token_price("MCX", "DUCO")
                mcx_amount = int(expected_amount * mcx_price) if mcx_price > 0 else int(expected_amount * 10)
                
                self.duco_pool_balance += expected_amount
                return {
                    "success": True, 
                    "received": expected_amount, 
                    "txid": txid, 
                    "memo": memo,
                    "mcx_amount": mcx_amount,
                    "price": mcx_price
                }
            
            return {"success": False, "error": "DUCO verification failed"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _update_bridge_balance(self, token_symbol: str, balance: float):
        c = self.net.conn.cursor()
        c.execute("UPDATE real_token_bridges SET balance=?, last_updated=? WHERE token_symbol=?",
                 (balance, time.time(), token_symbol))
        self.net.conn.commit()
        self.tokens[token_symbol]["balance"] = balance
        self.tokens[token_symbol]["last_updated"] = time.time()
    
    def _save_multisig_proposal(self, proposal: MultisigProposal):
        c = self.net.conn.cursor()
        c.execute("INSERT OR REPLACE INTO multisig_proposals (proposal_id, token_symbol, action, amount, recipient_wallet, signatures, status, created_at, tx_hash, memo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                 (proposal.proposal_id, proposal.token_symbol, proposal.action, proposal.amount,
                  proposal.recipient_wallet, json.dumps(proposal.signatures), proposal.status,
                  proposal.created_at, proposal.tx_hash, proposal.memo))
        self.net.conn.commit()
    
    def get_bridge_status(self) -> dict:
        status = {}
        for symbol, token in self.tokens.items():
            status[symbol] = {
                "address": token["bridge_address"],
                "balance": token["balance"],
                "last_updated": token.get("last_updated", 0),
                "multisig_threshold": token.get("multisig_threshold", self.threshold),
                "multisig_signers": len(self.get_multisig_signers())
            }
        status["DUCO_POOL"] = {
            "address": DUCO_PAYMENT_ADDRESS,
            "balance": self.duco_pool_balance,
            "last_updated": time.time()
        }
        return status
    
    def get_duco_pool_balance(self) -> float:
        return self.duco_pool_balance
    
    def get_multisig_proposals(self, status: str = "pending") -> List[dict]:
        c = self.net.conn.cursor()
        c.execute("SELECT * FROM multisig_proposals WHERE status=?", (status,))
        rows = c.fetchall()
        return [
            {
                "proposal_id": row[0],
                "token_symbol": row[1],
                "action": row[2],
                "amount": row[3],
                "recipient_wallet": row[4],
                "signatures": json.loads(row[5]),
                "status": row[6],
                "created_at": row[7],
                "tx_hash": row[8],
                "memo": row[9] if len(row) > 9 else ""
            }
            for row in rows
        ]
    
# ==================== DEX ====================
class DEX:
    def __init__(self, net):
        self.net = net
        self.pools = {}
        self.tokens = {}
        self.price_history = {}
        self.node_wallet = None
        self._lock = asyncio.Lock()
        self.fee_rate = SWAP_FEE_RATE
        self._load_tokens()
        self._load_pools()
        self._load_price_history()
        self._init_node_wallet()
    
    def _init_node_wallet(self):
        try:
            c = self.net.conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS node_wallet (
                address TEXT PRIMARY KEY,
                private_key_encrypted TEXT NOT NULL,
                public_key TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_updated REAL DEFAULT 0
            )''')
            self.net.conn.commit()
            c.execute("SELECT address, private_key_encrypted, public_key FROM node_wallet LIMIT 1")
            row = c.fetchone()
            if row:
                self.node_wallet = {
                    "address": row[0],
                    "private_key_encrypted": row[1],
                    "public_key_pem": row[2]
                }
            else:
                address, priv_hex, pub_pem = generate_wallet()
                encrypted_priv = encrypt_private_key(priv_hex, "dex_wallet_microcore_v15")
                c.execute("INSERT INTO node_wallet (address, private_key_encrypted, public_key, created_at) VALUES (?, ?, ?, ?)",
                         (address, json.dumps(encrypted_priv), pub_pem, time.time()))
                self.net.conn.commit()
                self.node_wallet = {
                    "address": address,
                    "private_key_encrypted": encrypted_priv,
                    "public_key_pem": pub_pem
                }
        except Exception as e:
            logger.error(f"[DEX] Failed to init node wallet: {e}")
            self.node_wallet = None
    
    def _load_tokens(self):
        try:
            c = self.net.conn.cursor()
            c.execute("SELECT symbol, name, supply, owner, created_at, decimals, mintable FROM tokens")
            rows = c.fetchall()
            for row in rows:
                self.tokens[row[0]] = {
                    "name": row[1],
                    "supply": row[2],
                    "owner": row[3],
                    "created_at": row[4],
                    "decimals": row[5] if len(row) > 5 else 18,
                    "mintable": bool(row[6]) if len(row) > 6 else False
                }
        except Exception as e:
            self.tokens = {}
    
    def _load_pools(self):
        try:
            c = self.net.conn.cursor()
            c.execute("SELECT pool_id, token_a, token_b, reserve_a, reserve_b, total_lp, created_at, fee_rate, volume_24h, swap_count, last_updated FROM pools")
            rows = c.fetchall()
            for row in rows:
                self.pools[row[0]] = {
                    "token_a": row[1],
                    "token_b": row[2],
                    "reserve_a": row[3],
                    "reserve_b": row[4],
                    "total_lp": row[5],
                    "created_at": row[6],
                    "fee_rate": row[7] if len(row) > 7 else 0.003,
                    "volume_24h": row[8] if len(row) > 8 else 0,
                    "swap_count": row[9] if len(row) > 9 else 0,
                    "last_updated": row[10] if len(row) > 10 else 0,
                    "lp_positions": {}
                }
                c2 = self.net.conn.cursor()
                c2.execute("SELECT wallet, lp_shares, created_at, fees_earned FROM lp_positions WHERE pool_id=?", (row[0],))
                for lp_row in c2.fetchall():
                    self.pools[row[0]]["lp_positions"][lp_row[0]] = {
                        "shares": lp_row[1],
                        "created_at": lp_row[2],
                        "fees_earned": lp_row[3] if len(lp_row) > 3 else 0
                    }
        except Exception as e:
            self.pools = {}
    
    def _load_price_history(self):
        try:
            c = self.net.conn.cursor()
            c.execute("SELECT pool_id, price, timestamp FROM price_history ORDER BY timestamp DESC LIMIT 1000")
            rows = c.fetchall()
            for row in rows:
                if row[0] not in self.price_history:
                    self.price_history[row[0]] = []
                self.price_history[row[0]].append({"price": row[1], "timestamp": row[2]})
        except Exception as e:
            self.price_history = {}
    
    def get_token_price(self, token_a: str, token_b: str) -> float:
        """Get token price from DEX pool"""
        pool_id = f"{token_a}/{token_b}"
        reverse_pool_id = f"{token_b}/{token_a}"
        
        pool = self.pools.get(pool_id) or self.pools.get(reverse_pool_id)
        if not pool:
            return 0.0
        
        if pool["token_a"] == token_a and pool["token_b"] == token_b:
            if pool["reserve_b"] > 0:
                return pool["reserve_a"] / pool["reserve_b"]
        elif pool["token_b"] == token_a and pool["token_a"] == token_b:
            if pool["reserve_a"] > 0:
                return pool["reserve_b"] / pool["reserve_a"]
        
        return 0.0
    
    def _get_token_balance(self, wallet: str, symbol: str) -> int:
        if symbol == "MCX":
            return self.net.balances.get(wallet, 0)
        c = self.net.conn.cursor()
        c.execute("SELECT balance FROM token_balances WHERE wallet=? AND token_symbol=?", (wallet, symbol))
        row = c.fetchone()
        return row[0] if row else 0
    
    def _mint_token(self, symbol: str, to: str, amount: int) -> dict:
        if symbol == "MCX":
            self.net.balances[to] = self.net.balances.get(to, 0) + amount
            self.net._save_balance(to, self.net.balances[to])
            return {"success": True}
        c = self.net.conn.cursor()
        c.execute("INSERT OR REPLACE INTO token_balances (wallet, token_symbol, balance) VALUES (?, ?, COALESCE((SELECT balance FROM token_balances WHERE wallet=? AND token_symbol=?), 0) + ?)",
                  (to, symbol, to, symbol, amount))
        self.net.conn.commit()
        return {"success": True}
    
    def _burn_token(self, symbol: str, from_wallet: str, amount: int) -> dict:
        if symbol == "MCX":
            if self.net.balances.get(from_wallet, 0) < amount:
                return {"success": False, "error": "Insufficient balance"}
            self.net.balances[from_wallet] -= amount
            self.net._save_balance(from_wallet, self.net.balances[from_wallet])
            return {"success": True}
        balance = self._get_token_balance(from_wallet, symbol)
        if balance < amount:
            return {"success": False, "error": "Insufficient token balance"}
        c = self.net.conn.cursor()
        c.execute("UPDATE token_balances SET balance = balance - ? WHERE wallet=? AND token_symbol=?",
                  (amount, from_wallet, symbol))
        self.net.conn.commit()
        return {"success": True}
    
    def _calculate_gas_fee(self, amount: int) -> int:
        return calculate_dex_gas_fee(amount)
    
    def _record_price(self, pool_id: str, price: float, volume: int):
        try:
            c = self.net.conn.cursor()
            c.execute("INSERT INTO price_history (pool_id, price, timestamp, volume) VALUES (?, ?, ?, ?)",
                      (pool_id, price, time.time(), volume))
            self.net.conn.commit()
            if pool_id not in self.price_history:
                self.price_history[pool_id] = []
            self.price_history[pool_id].append({"price": price, "timestamp": time.time()})
            if len(self.price_history[pool_id]) > 1000:
                self.price_history[pool_id] = self.price_history[pool_id][-1000:]
        except Exception as e:
            pass
    
    def _update_pool_in_db(self, pool_id: str):
        pool = self.pools.get(pool_id)
        if not pool:
            return
        c = self.net.conn.cursor()
        c.execute("UPDATE pools SET reserve_a=?, reserve_b=?, last_updated=? WHERE pool_id=?",
                 (pool["reserve_a"], pool["reserve_b"], time.time(), pool_id))
        self.net.conn.commit()
    
    async def create_pool(self, token_a: str, token_b: str, amount_a: int, amount_b: int, creator: str) -> dict:
        async with self._lock:
            if token_a == token_b:
                return {"success": False, "error": "Cannot create pool with same token"}
            
            pool_id = f"{token_a}/{token_b}"
            pool_id_reverse = f"{token_b}/{token_a}"
            if pool_id in self.pools or pool_id_reverse in self.pools:
                return {"success": False, "error": "Pool already exists"}
            
            if token_a not in self.tokens and token_a != "MCX":
                return {"success": False, "error": f"Token {token_a} doesn't exist"}
            if token_b not in self.tokens and token_b != "MCX":
                return {"success": False, "error": f"Token {token_b} doesn't exist"}
            
            total_amount = amount_a + amount_b
            gas_fee = self._calculate_gas_fee(total_amount)
            if self.net.balances.get(creator, 0) < gas_fee:
                return {"success": False, "error": f"Insufficient MCX for gas fee. Need {gas_fee}"}
            
            bal_a = self._get_token_balance(creator, token_a)
            bal_b = self._get_token_balance(creator, token_b)
            if bal_a < amount_a:
                return {"success": False, "error": f"Insufficient {token_a} balance"}
            if bal_b < amount_b:
                return {"success": False, "error": f"Insufficient {token_b} balance"}
            
            self.net.balances[creator] -= gas_fee
            validator_share = int(gas_fee * 0.70)
            node_share = gas_fee - validator_share
            self.net.validator_fee_pool += validator_share
            self.net.node_pool += node_share
            self.net._save_balance(creator, self.net.balances[creator])
            
            lp_shares = int((amount_a * amount_b) ** 0.5)
            if lp_shares < 1:
                return {"success": False, "error": "Amount too small, LP shares would be 0"}
            
            if token_a == "MCX":
                self.net.balances[creator] -= amount_a
                self.net._save_balance(creator, self.net.balances[creator])
            else:
                self._burn_token(token_a, creator, amount_a)
            
            if token_b == "MCX":
                self.net.balances[creator] -= amount_b
                self.net._save_balance(creator, self.net.balances[creator])
            else:
                self._burn_token(token_b, creator, amount_b)
            
            self.pools[pool_id] = {
                "token_a": token_a,
                "token_b": token_b,
                "reserve_a": amount_a,
                "reserve_b": amount_b,
                "total_lp": lp_shares,
                "created_at": time.time(),
                "fee_rate": self.fee_rate,
                "volume_24h": 0,
                "swap_count": 0,
                "last_updated": time.time(),
                "lp_positions": {creator: {"shares": lp_shares, "created_at": time.time(), "fees_earned": 0}}
            }
            
            c = self.net.conn.cursor()
            c.execute("INSERT INTO pools (pool_id, token_a, token_b, reserve_a, reserve_b, total_lp, created_at, fee_rate, volume_24h, swap_count, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (pool_id, token_a, token_b, amount_a, amount_b, lp_shares, time.time(), self.fee_rate, 0, 0, time.time()))
            c.execute("INSERT INTO lp_positions (pool_id, wallet, lp_shares, created_at, fees_earned) VALUES (?, ?, ?, ?, ?)",
                      (pool_id, creator, lp_shares, time.time(), 0))
            self.net.conn.commit()
            
            price = amount_a / amount_b if amount_b > 0 else 0
            self._record_price(pool_id, price, 0)
            
            logger.info(f"[DEX] Pool created: {pool_id} with {lp_shares} LP shares (price: {price})")
            return {"success": True, "pool_id": pool_id, "lp_shares": lp_shares, "price": price}
    
    async def add_liquidity(self, pool_id: str, amount_a: int, amount_b: int, user: str) -> dict:
        async with self._lock:
            if pool_id not in self.pools:
                return {"success": False, "error": "Pool not found"}
            
            pool = self.pools[pool_id]
            token_a = pool["token_a"]
            token_b = pool["token_b"]
            total_amount = amount_a + amount_b
            gas_fee = self._calculate_gas_fee(total_amount)
            
            if self.net.balances.get(user, 0) < gas_fee:
                return {"success": False, "error": f"Insufficient MCX for gas fee. Need {gas_fee}"}
            
            bal_a = self._get_token_balance(user, token_a)
            bal_b = self._get_token_balance(user, token_b)
            if bal_a < amount_a:
                return {"success": False, "error": f"Insufficient {token_a} balance"}
            if bal_b < amount_b:
                return {"success": False, "error": f"Insufficient {token_b} balance"}
            
            self.net.balances[user] -= gas_fee
            validator_share = int(gas_fee * 0.70)
            node_share = gas_fee - validator_share
            self.net.validator_fee_pool += validator_share
            self.net.node_pool += node_share
            self.net._save_balance(user, self.net.balances[user])
            
            if pool["total_lp"] == 0:
                lp_shares = int((amount_a * amount_b) ** 0.5)
            else:
                shares_a = (amount_a * pool["total_lp"]) // pool["reserve_a"] if pool["reserve_a"] > 0 else 0
                shares_b = (amount_b * pool["total_lp"]) // pool["reserve_b"] if pool["reserve_b"] > 0 else 0
                lp_shares = min(shares_a, shares_b)
            
            if lp_shares < 1:
                return {"success": False, "error": "LP shares would be 0"}
            
            if token_a == "MCX":
                self.net.balances[user] -= amount_a
                self.net._save_balance(user, self.net.balances[user])
            else:
                self._burn_token(token_a, user, amount_a)
            
            if token_b == "MCX":
                self.net.balances[user] -= amount_b
                self.net._save_balance(user, self.net.balances[user])
            else:
                self._burn_token(token_b, user, amount_b)
            
            pool["reserve_a"] += amount_a
            pool["reserve_b"] += amount_b
            pool["total_lp"] += lp_shares
            pool["last_updated"] = time.time()
            
            if user not in pool["lp_positions"]:
                pool["lp_positions"][user] = {"shares": 0, "created_at": time.time(), "fees_earned": 0}
            pool["lp_positions"][user]["shares"] += lp_shares
            
            c = self.net.conn.cursor()
            c.execute("UPDATE pools SET reserve_a=?, reserve_b=?, total_lp=?, last_updated=? WHERE pool_id=?",
                      (pool["reserve_a"], pool["reserve_b"], pool["total_lp"], time.time(), pool_id))
            c.execute("INSERT OR REPLACE INTO lp_positions (pool_id, wallet, lp_shares, created_at, fees_earned) VALUES (?, ?, ?, ?, ?)",
                      (pool_id, user, pool["lp_positions"][user]["shares"], pool["lp_positions"][user]["created_at"], pool["lp_positions"][user]["fees_earned"]))
            self.net.conn.commit()
            
            price = pool["reserve_a"] / pool["reserve_b"] if pool["reserve_b"] > 0 else 0
            self._record_price(pool_id, price, amount_a + amount_b)
            
            logger.info(f"[DEX] Added liquidity to {pool_id}: +{lp_shares} LP shares (price: {price})")
            return {"success": True, "pool_id": pool_id, "lp_shares": lp_shares, "total_lp": pool["total_lp"], "price": price}
    
    async def swap(self, pool_id: str, token_in: str, token_out: str, amount_in: int, user: str, slippage: float = 0.005) -> dict:
        async with self._lock:
            if pool_id not in self.pools:
                return {"success": False, "error": "Pool not found"}
            
            pool = self.pools[pool_id]
            if token_in not in [pool["token_a"], pool["token_b"]]:
                return {"success": False, "error": f"Token {token_in} not in pool"}
            if token_out not in [pool["token_a"], pool["token_b"]]:
                return {"success": False, "error": f"Token {token_out} not in pool"}
            if token_in == token_out:
                return {"success": False, "error": "Cannot swap same token"}
            
            gas_fee = self._calculate_gas_fee(amount_in)
            if token_in == pool["token_a"] and token_out == pool["token_b"]:
                reserve_in = pool["reserve_a"]
                reserve_out = pool["reserve_b"]
            else:
                reserve_in = pool["reserve_b"]
                reserve_out = pool["reserve_a"]
            
            bal = self._get_token_balance(user, token_in)
            if bal < amount_in + gas_fee:
                return {"success": False, "error": f"Insufficient {token_in} balance"}
            
            if token_in == "MCX":
                self.net.balances[user] -= gas_fee
                self.net._save_balance(user, self.net.balances[user])
            else:
                self._burn_token(token_in, user, gas_fee)
            
            validator_share = int(gas_fee * 0.70)
            node_share = gas_fee - validator_share
            self.net.validator_fee_pool += validator_share
            self.net.node_pool += node_share
            
            fee = int(amount_in * self.fee_rate)
            protocol_fee = int(fee * 0.10)
            lp_fee = fee - protocol_fee
            
            amount_in_without_fee = amount_in - fee
            amount_out = (amount_in_without_fee * reserve_out) // (reserve_in + amount_in_without_fee)
            
            if amount_out <= 0:
                return {"success": False, "error": "Amount out too small"}
            
            quote = self.get_quote(pool_id, token_in, token_out, amount_in)
            expected_out = quote.get("amount_out", 0)
            if expected_out > 0:
                slippage_actual = abs(amount_out - expected_out) / expected_out
                if slippage_actual > slippage:
                    return {"success": False, "error": f"Slippage too high: {slippage_actual*100:.2f}% > {slippage*100:.2f}%"}
            
            if token_in == "MCX":
                self.net.balances[user] -= amount_in
                self.net._save_balance(user, self.net.balances[user])
            else:
                self._burn_token(token_in, user, amount_in)
            
            if token_out == "MCX":
                self.net.balances[user] = self.net.balances.get(user, 0) + amount_out
                self.net._save_balance(user, self.net.balances[user])
            else:
                self._mint_token(token_out, user, amount_out)
            
            if token_in == pool["token_a"]:
                pool["reserve_a"] += amount_in
                pool["reserve_b"] -= amount_out
            else:
                pool["reserve_b"] += amount_in
                pool["reserve_a"] -= amount_out
            
            self.net.lp_pool += lp_fee
            if self.node_wallet:
                self.net.balances[self.node_wallet["address"]] = self.net.balances.get(self.node_wallet["address"], 0) + protocol_fee
            
            pool["swap_count"] += 1
            pool["volume_24h"] += amount_in
            pool["last_updated"] = time.time()
            self._update_pool_in_db(pool_id)
            
            price = amount_in / amount_out if amount_out > 0 else 0
            self._record_price(pool_id, price, amount_in + amount_out)
            
            logger.info(f"[DEX] Swap: {amount_in} {token_in} -> {amount_out} {token_out} (price: {price})")
            return {"success": True, "token_in": token_in, "token_out": token_out, "amount_in": amount_in, "amount_out": amount_out, "fee": fee, "gas_fee": gas_fee, "price": price}
    
    def get_quote(self, pool_id: str, token_in: str, token_out: str, amount_in: int) -> dict:
        if pool_id not in self.pools:
            return {"success": False, "error": "Pool not found"}
        
        pool = self.pools[pool_id]
        if token_in not in [pool["token_a"], pool["token_b"]]:
            return {"success": False, "error": f"Token {token_in} not in pool"}
        if token_out not in [pool["token_a"], pool["token_b"]]:
            return {"success": False, "error": f"Token {token_out} not in pool"}
        if token_in == token_out:
            return {"success": False, "error": "Cannot swap same token"}
        
        if token_in == pool["token_a"] and token_out == pool["token_b"]:
            reserve_in = pool["reserve_a"]
            reserve_out = pool["reserve_b"]
        else:
            reserve_in = pool["reserve_b"]
            reserve_out = pool["reserve_a"]
        
        gas_fee = self._calculate_gas_fee(amount_in)
        fee = int(amount_in * self.fee_rate)
        amount_in_without_fee = amount_in - fee
        amount_out = (amount_in_without_fee * reserve_out) // (reserve_in + amount_in_without_fee)
        price = amount_in / amount_out if amount_out > 0 else 0
        
        return {"success": True, "pool_id": pool_id, "amount_out": amount_out, "fee": fee, "gas_fee": gas_fee, "price": price}
    
    def get_pools(self) -> dict:
        pools_data = []
        for pool_id, pool in self.pools.items():
            price = pool["reserve_a"] / pool["reserve_b"] if pool["reserve_b"] > 0 else 0
            pools_data.append({
                "pool_id": pool_id,
                "token_a": pool["token_a"],
                "token_b": pool["token_b"],
                "reserve_a": pool["reserve_a"],
                "reserve_b": pool["reserve_b"],
                "total_lp": pool["total_lp"],
                "price": price,
                "liquidity_providers": len(pool["lp_positions"]),
                "volume_24h": pool["volume_24h"],
                "swap_count": pool["swap_count"],
                "created_at": pool["created_at"],
                "fee_rate": pool["fee_rate"]
            })
        return {"success": True, "pools": pools_data}
    
    def get_user_lp(self, wallet: str) -> dict:
        positions = []
        for pool_id, pool in self.pools.items():
            if wallet in pool["lp_positions"]:
                lp_data = pool["lp_positions"][wallet]
                shares = lp_data["shares"]
                if shares > 0:
                    ratio = shares / pool["total_lp"] if pool["total_lp"] > 0 else 0
                    positions.append({
                        "pool_id": pool_id,
                        "token_a": pool["token_a"],
                        "token_b": pool["token_b"],
                        "lp_shares": shares,
                        "share_percentage": ratio * 100,
                        "amount_a": int(pool["reserve_a"] * ratio),
                        "amount_b": int(pool["reserve_b"] * ratio),
                        "fees_earned": lp_data["fees_earned"],
                        "created_at": lp_data["created_at"]
                    })
        return {"success": True, "wallet": wallet, "positions": positions}
    
    def get_tokens(self) -> dict:
        tokens_data = []
        for symbol, token in self.tokens.items():
            tokens_data.append({
                "symbol": symbol,
                "name": token["name"],
                "supply": token["supply"],
                "owner": token["owner"],
                "created_at": token["created_at"],
                "decimals": token["decimals"],
                "mintable": token["mintable"]
            })
        tokens_data.append({
            "symbol": "MCX",
            "name": "MicroCore Token",
            "supply": self.net.total_minted,
            "owner": "genesis",
            "created_at": 0,
            "decimals": 18,
            "mintable": False
        })
        return {"success": True, "tokens": tokens_data}
    
    def get_user_token_balances(self, wallet: str) -> dict:
        balances = []
        mcx_balance = self.net.balances.get(wallet, 0)
        if mcx_balance > 0:
            balances.append({"symbol": "MCX", "balance": mcx_balance})
        c = self.net.conn.cursor()
        c.execute("SELECT token_symbol, balance FROM token_balances WHERE wallet=?", (wallet,))
        rows = c.fetchall()
        for row in rows:
            balances.append({"symbol": row[0], "balance": row[1]})
        return {"success": True, "wallet": wallet, "balances": balances}
    
    def get_price_history(self, pool_id: str, hours: int = 24) -> dict:
        if pool_id not in self.pools:
            return {"success": False, "error": "Pool not found"}
        
        cutoff = time.time() - (hours * 3600)
        c = self.net.conn.cursor()
        c.execute("SELECT price, timestamp, volume FROM price_history WHERE pool_id=? AND timestamp > ? ORDER BY timestamp ASC",
                  (pool_id, cutoff))
        rows = c.fetchall()
        history = [{"price": row[0], "timestamp": row[1], "volume": row[2]} for row in rows]
        
        if history:
            prices = [h["price"] for h in history]
            current_price = prices[-1]
            high = max(prices)
            low = min(prices)
            avg = sum(prices) / len(prices)
            volume_24h = sum(h["volume"] for h in history)
        else:
            current_price = self.pools[pool_id]["reserve_a"] / self.pools[pool_id]["reserve_b"] if self.pools[pool_id]["reserve_b"] > 0 else 0
            high = low = avg = current_price
            volume_24h = 0
        
        return {
            "success": True,
            "pool_id": pool_id,
            "history": history,
            "current_price": current_price,
            "high_24h": high,
            "low_24h": low,
            "avg_price": avg,
            "volume_24h": volume_24h
        }
    
    async def create_genesis_pools(self):
        """Create genesis pools with ZERO starting price (no liquidity)"""
        mcx_amount = 0
        
        pools = [
            ("MCX", "USDT"),
            ("MCX", "BTC"),
            ("MCX", "POL"),
            ("MCX", "TRX"),
            ("MCX", "DUCO")
        ]
        
        for token_a, token_b in pools:
            result = await self.create_pool(token_a, token_b, 0, 0, self.net.wallet)
            if result["success"]:
                logger.info(f"[GENESIS] Created {token_a}/{token_b} pool (ZERO liquidity)")
            else:
                logger.info(f"[GENESIS] Pool {token_a}/{token_b} already exists")
        
        logger.info("[GENESIS] All pools created with ZERO liquidity. Price discovery starts when users add liquidity.")

# ==================== LEVEL MANAGER ====================
class LevelManager:
    def __init__(self, net):
        self.net = net
        self.max_unlocked = 1
        self.temp_towers: Dict[str, Dict[int, int]] = {}
        self.perm_towers: Dict[str, int] = {}
        self.level_wallets: Dict[int, Set[str]] = defaultdict(set)
        self.level_counts: Dict[int, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        self._load_towers()
    
    def _load_towers(self):
        try:
            c = self.net.conn.cursor()
            c.execute("SELECT wallet, level, amount, type, created_at, expires_at FROM towers")
            rows = c.fetchall()
            for row in rows:
                wallet, level, amount, tower_type, created_at, expires_at = row
                if tower_type == "temporary":
                    if wallet not in self.temp_towers:
                        self.temp_towers[wallet] = {}
                    self.temp_towers[wallet][level] = amount
                else:
                    self.perm_towers[wallet] = self.perm_towers.get(wallet, 0) + amount
            logger.info(f"[LEVEL] Loaded {len(self.temp_towers)} temporary towers, {len(self.perm_towers)} permanent towers")
        except Exception as e:
            self.temp_towers = {}
            self.perm_towers = {}
    
    def _save_tower(self, wallet: str, level: int, amount: int, tower_type: str):
        c = self.net.conn.cursor()
        c.execute("INSERT OR REPLACE INTO towers (wallet, level, amount, type, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
                  (wallet, level, amount, tower_type, time.time(), 0 if tower_type == "permanent" else time.time() + 86400 * 30))
        self.net.conn.commit()
    
    def register(self, wallet: str, stake: int):
        alloc = {}
        rem = stake
        lvl = 1
        
        while rem > 0 and lvl <= self.max_unlocked:
            add = min(rem, LEVEL_STAKE_RANGE)
            alloc[lvl] = alloc.get(lvl, 0) + add
            rem -= add
            lvl += 1
        
        if rem > 0 and lvl <= MAX_LEVEL:
            for lock_lvl in range(lvl, MAX_LEVEL + 1):
                if rem <= 0:
                    break
                add = min(rem, LEVEL_STAKE_RANGE)
                
                if lock_lvl not in self.temp_towers:
                    self.temp_towers[lock_lvl] = {}
                self.temp_towers[lock_lvl][wallet] = self.temp_towers[lock_lvl].get(wallet, 0) + add
                rem -= add
                self._save_tower(wallet, lock_lvl, add, "temporary")
        
        if rem > 0:
            self.perm_towers[wallet] = self.perm_towers.get(wallet, 0) + rem
            self._save_tower(wallet, MAX_LEVEL + 1, rem, "permanent")
        
        self._update()
    
    def _update(self):
        self.level_wallets.clear()
        self.level_counts.clear()
        
        for miner in self.net.miners.values():
            if miner.active:
                lvl = self.get_level(miner.wallet)
                self.level_wallets[lvl].add(miner.wallet)
        
        for lvl in range(1, MAX_LEVEL + 1):
            self.level_counts[lvl] = len(self.level_wallets.get(lvl, set()))
        
        while self.max_unlocked < MAX_LEVEL:
            next_level = self.max_unlocked + 1
            if self.level_counts.get(self.max_unlocked, 0) >= MIN_WALLETS_FOR_NEXT_LEVEL:
                self.max_unlocked = next_level
                logger.info(f"[LEVEL] Level {self.max_unlocked} UNLOCKED! ({self.level_counts.get(self.max_unlocked, 0)} different wallets)")
                self._convert_temporary_towers()
            else:
                break
    
    def _convert_temporary_towers(self):
        for wallet, towers in list(self.temp_towers.items()):
            if self.max_unlocked in towers:
                stake = towers[self.max_unlocked]
                
                for miner in self.net.miners.values():
                    if miner.wallet == wallet:
                        miner.stake += stake
                        miner.level = self.get_level(wallet)
                        break
                
                del towers[self.max_unlocked]
                self.perm_towers[wallet] = self.perm_towers.get(wallet, 0) + stake
                self._save_tower(wallet, self.max_unlocked, stake, "permanent")
                logger.info(f"[LEVEL] Converted tower for {wallet[:16]}... at Level {self.max_unlocked} (+{stake} MCX)")
    
    def get_level(self, wallet: str) -> int:
        for miner in self.net.miners.values():
            if miner.wallet == wallet:
                if miner.stake >= LEVEL_STAKE_RANGE:
                    return min((miner.stake - 1) // LEVEL_STAKE_RANGE + 1, MAX_LEVEL)
                return 1
        return 1
    
    def get_tower_stake(self, wallet: str) -> int:
        total = 0
        total += self.perm_towers.get(wallet, 0)
        for level in self.temp_towers.values():
            if wallet in level:
                total += level[wallet]
        return total
    
    def get_level_stats(self) -> dict:
        return {
            "max_unlocked": self.max_unlocked,
            "levels": {
                str(lvl): {
                    "wallets": self.level_counts.get(lvl, 0),
                    "required": MIN_WALLETS_FOR_NEXT_LEVEL,
                    "unlocked": lvl <= self.max_unlocked,
                    "block_interval": LEVEL_BLOCK_INTERVALS.get(lvl, 40),
                    "different_wallets": list(self.level_wallets.get(lvl, set()))[:5]
                }
                for lvl in range(1, MAX_LEVEL + 1)
            },
            "temporary_towers": len(self.temp_towers),
            "permanent_towers": len(self.perm_towers)
        }

# ==================== P2P NODE ====================
class P2PNode:
    def __init__(self, net):
        self.net = net
        self.peers: Dict[str, any] = {}
        self.banned_peers: Dict[str, float] = {}
        self.ip = get_public_ip() or "127.0.0.1"
        self._lock = asyncio.Lock()
        self._server = None
        self._metrics = {
            "messages_sent": 0,
            "messages_received": 0,
            "bytes_sent": 0,
            "bytes_received": 0,
            "connections": 0,
            "errors": 0,
            "peers_discovered": 0
        }
    
    async def start(self):
        self._server = await asyncio.start_server(self._handle_connection, "0.0.0.0", P2P_PORT, limit=100)
        logger.info(f"[P2P] Server on port {P2P_PORT}")
    
    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        addr_str = f"{addr[0]}:{addr[1]}"
        
        if addr_str in self.banned_peers and time.time() < self.banned_peers[addr_str]:
            writer.close()
            await writer.wait_closed()
            return
        
        self._metrics["connections"] += 1
        
        try:
            length_data = await reader.read(4)
            if not length_data:
                writer.close()
                return
            
            msg_len = struct.unpack(">I", length_data)[0]
            if msg_len > 10_000_000:
                self.banned_peers[addr_str] = time.time() + BAN_DURATION
                writer.close()
                return
            
            data = await reader.read(msg_len)
            self._metrics["bytes_received"] += len(data)
            msg_type, payload = decode_p2p(data)
            
            if msg_type is not None:
                self._metrics["messages_received"] += 1
                await self._process_message(msg_type, payload, writer, addr_str)
                
        except Exception as e:
            logger.error(f"[P2P] Error handling {addr_str}: {e}")
            self._metrics["errors"] += 1
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def _process_message(self, msg_type: int, payload: dict, writer: asyncio.StreamWriter, addr: str):
        try:
            if msg_type == MSG_HANDSHAKE:
                async with self._lock:
                    if addr not in self.peers:
                        self.peers[addr] = type('Peer', (), {
                            'height': payload.get('height', 0),
                            'last_seen': time.time(),
                            'version': payload.get('version', 'unknown')
                        })
                        self._metrics["peers_discovered"] += 1
                
                response = encode_p2p(MSG_HANDSHAKE, {
                    "height": self.net.height,
                    "ip": self.ip,
                    "version": VERSION
                })
                writer.write(struct.pack(">I", len(response)) + response)
                await writer.drain()
                self._metrics["messages_sent"] += 1
                
                if payload.get('height', 0) > self.net.height:
                    asyncio.create_task(self._request_blocks(addr, self.net.height, payload['height']))
            
            elif msg_type == MSG_GET_PEERS:
                async with self._lock:
                    peers_list = list(self.peers.keys())[:100]
                
                response = encode_p2p(MSG_PEERS, {"peers": peers_list})
                writer.write(struct.pack(">I", len(response)) + response)
                await writer.drain()
                self._metrics["messages_sent"] += 1
            
            elif msg_type == MSG_PEERS:
                async with self._lock:
                    for peer in payload.get("peers", []):
                        if peer not in self.peers and peer != f"{self.ip}:{P2P_PORT}":
                            self.peers[peer] = type('Peer', (), {
                                'height': 0,
                                'last_seen': time.time(),
                                'version': 'unknown'
                            })
                            asyncio.create_task(self._connect(peer))
            
            elif msg_type == MSG_GET_BLOCKS:
                start = payload.get("start", 0)
                end = payload.get("end", self.net.height)
                if end - start > 2000:
                    end = start + 2000
                blocks = self.net.get_blocks_range(start, end)
                response = encode_p2p(MSG_BLOCKS, {"blocks": blocks})
                writer.write(struct.pack(">I", len(response)) + response)
                await writer.drain()
                self._metrics["messages_sent"] += 1
            
            elif msg_type == MSG_BLOCKS:
                await self.net.import_blocks(payload.get("blocks", []))
            
            elif msg_type == MSG_NEW_BLOCK:
                await self.net.receive_block(payload.get("block"))
            
            elif msg_type == MSG_NEW_TX:
                await self.net.receive_transaction(payload.get("tx"))
            
            elif msg_type == MSG_SLASH:
                self.net.slash_miner(payload.get("vid"), "P2P slashing event")
            
            elif msg_type == MSG_GET_STATUS:
                status = {
                    "height": self.net.height,
                    "peers": len(self.peers),
                    "miners": len(self.net.miners),
                    "version": VERSION
                }
                response = encode_p2p(MSG_STATUS, status)
                writer.write(struct.pack(">I", len(response)) + response)
                await writer.drain()
                self._metrics["messages_sent"] += 1
            
            elif msg_type == MSG_BAN:
                vid = payload.get("vid")
                if vid in self.net.miners:
                    self.net.miners[vid].active = False
                    self.net.miners[vid].banned_until = time.time() + BAN_DURATION
                    self.net.conn.execute("UPDATE miners SET active=0, banned_until=? WHERE vid=?", (time.time() + BAN_DURATION, vid))
                    self.net.conn.commit()
            
            elif msg_type == MSG_UNBAN:
                vid = payload.get("vid")
                if vid in self.net.miners:
                    self.net.miners[vid].active = True
                    self.net.miners[vid].banned_until = 0
                    self.net.conn.execute("UPDATE miners SET active=1, banned_until=0 WHERE vid=?", (vid,))
                    self.net.conn.commit()
                    
        except Exception as e:
            logger.error(f"[P2P] Process message error: {e}")
    
    async def _request_blocks(self, peer: str, start: int, end: int):
        try:
            h, p = peer.split(":")
            reader, writer = await asyncio.open_connection(h, int(p))
            msg = encode_p2p(MSG_GET_BLOCKS, {"start": start, "end": end})
            writer.write(struct.pack(">I", len(msg)) + msg)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            self._metrics["messages_sent"] += 1
        except Exception as e:
            logger.error(f"[P2P] Request blocks failed: {e}")
    
    async def broadcast_block(self, block: dict):
        msg = encode_p2p(MSG_NEW_BLOCK, {"block": block})
        await self._broadcast_message(msg)
        self._metrics["messages_sent"] += 1
    
    async def broadcast_transaction(self, tx: dict):
        msg = encode_p2p(MSG_NEW_TX, {"tx": tx})
        await self._broadcast_message(msg)
        self._metrics["messages_sent"] += 1
    
    async def broadcast_slash(self, vid: str):
        msg = encode_p2p(MSG_SLASH, {"vid": vid})
        await self._broadcast_message(msg)
        self._metrics["messages_sent"] += 1
    
    async def _broadcast_message(self, msg: bytes):
        async with self._lock:
            peers_copy = list(self.peers.keys())
        
        for peer in peers_copy:
            try:
                h, p = peer.split(":")
                reader, writer = await asyncio.open_connection(h, int(p))
                writer.write(struct.pack(">I", len(msg)) + msg)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self._metrics["messages_sent"] += 1
            except:
                async with self._lock:
                    if peer in self.peers:
                        del self.peers[peer]
    
    async def discover(self):
        bootstrap = get_bootstrap_peers()
        logger.info(f"[P2P] Starting gossip discovery with {len(bootstrap)} bootstrap peers")
        
        for peer in bootstrap:
            async with self._lock:
                if peer not in self.peers:
                    asyncio.create_task(self._connect(peer))
        
        await asyncio.sleep(2)
        
        async with self._lock:
            peers_copy = list(self.peers.keys())
        
        for peer_addr in peers_copy[:10]:
            try:
                h, p = peer_addr.split(":")
                reader, writer = await asyncio.open_connection(h, int(p))
                msg = encode_p2p(MSG_GET_PEERS, {})
                writer.write(struct.pack(">I", len(msg)) + msg)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self._metrics["messages_sent"] += 1
            except:
                async with self._lock:
                    if peer_addr in self.peers:
                        del self.peers[peer_addr]
        
        async with self._lock:
            save_peers_to_cache(list(self.peers.keys()))
        
        logger.info(f"[P2P] Discovered {len(self.peers)} peers")
    
    async def _connect(self, addr: str):
        async with self._lock:
            if addr in self.peers:
                return
        
        try:
            h, p = addr.split(":")
            reader, writer = await asyncio.open_connection(h, int(p))
            msg = encode_p2p(MSG_HANDSHAKE, {
                "height": self.net.height,
                "ip": self.ip,
                "version": VERSION
            })
            writer.write(struct.pack(">I", len(msg)) + msg)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            
            async with self._lock:
                self.peers[addr] = type('Peer', (), {
                    'height': 0,
                    'last_seen': time.time(),
                    'version': 'unknown'
                })
                self._metrics["peers_discovered"] += 1
            
            logger.info(f"[P2P] Connected to peer: {addr}")
            
            async with self._lock:
                save_peers_to_cache(list(self.peers.keys()))
                
        except Exception as e:
            logger.debug(f"[P2P] Failed to connect to {addr}: {e}")
    
    async def sync_with_peers(self):
        async with self._lock:
            if not self.peers:
                return
            
            best_peer = None
            best_height = self.net.height
            
            for addr, peer in self.peers.items():
                if hasattr(peer, 'height') and peer.height > best_height:
                    best_height = peer.height
                    best_peer = addr
        
        if best_peer and best_height > self.net.height:
            logger.info(f"[P2P] Syncing from {best_peer}: local={self.net.height}, remote={best_height}")
            await self._request_blocks(best_peer, self.net.height, best_height)
    
    async def heartbeat(self):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            msg = encode_p2p(MSG_PING, {"timestamp": time.time()})
            
            async with self._lock:
                peers_copy = list(self.peers.keys())
            
            for peer_addr in peers_copy:
                try:
                    h, p = peer_addr.split(":")
                    reader, writer = await asyncio.open_connection(h, int(p))
                    writer.write(struct.pack(">I", len(msg)) + msg)
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                    self._metrics["messages_sent"] += 1
                except:
                    async with self._lock:
                        if peer_addr in self.peers:
                            del self.peers[peer_addr]
    
    def get_metrics(self) -> dict:
        return {
            "peers": len(self.peers),
            "banned": len(self.banned_peers),
            "peers_discovered": self._metrics["peers_discovered"],
            "messages_sent": self._metrics["messages_sent"],
            "messages_received": self._metrics["messages_received"],
            "bytes_sent": self._metrics["bytes_sent"],
            "bytes_received": self._metrics["bytes_received"],
            "connections": self._metrics["connections"],
            "errors": self._metrics["errors"]
        }
    
# ==================== MICROCORE NETWORK ====================
class MicroCoreNetwork:
    def __init__(self, is_genesis: bool, display_name: str, wallet: str, priv: str, pub: str, password: str = ""):
        self.miners: Dict[str, Miner] = {}
        self.nodes: Dict[str, Node] = {}
        self.balances: Dict[str, int] = {}
        self.blocks: List[Block] = []
        self.transactions: List[Transaction] = []
        self.height = 0
        self.last_hash = "0" * 64
        self.pending_challenges: Dict[str, dict] = {}
        self.tunnel_url = None
        
        self.miner_pool = 0
        self.node_pool = 0
        self.lp_pool = 0
        self.buyer_pool = 0
        self.uptime_pool = 0
        self.validator_fee_pool = 0
        
        self.total_minted = 0
        self.is_genesis = is_genesis
        self.display_name = display_name
        self.wallet = wallet
        self.priv = priv
        self.pub = pub
        self.password = password
        self.node_id = hashlib.sha256(f"{wallet}{time.time()}{secrets.token_hex(8)}".encode()).hexdigest()[:16]
        
        self.last_buyer_distribution = time.time()
        self.last_status_report = time.time()
        self.start_time = time.time()
        self._last_self_ping = time.time()
        
        self.level_groups: Dict[int, List[str]] = {i: [] for i in range(1, 11)}
        self.levels_with_miners: Set[int] = set()
        
        self.rate_limiter = RateLimiter()
        self.health_checker = HealthChecker()
        self.level_mgr = None
        self.p2p = P2PNode(self)
        self.mempool = Mempool()
        self.miners_cache: Dict[str, dict] = {}
        self.node_cache: Dict[str, dict] = {}
        self.metrics = {
            "blocks_produced": 0,
            "transactions_processed": 0,
            "challenges_sent": 0,
            "slash_events": 0,
            "rewards_distributed": 0,
            "mempool_size": 0,
            "peer_count": 0
        }
        
        self._init_db()
        self.level_mgr = LevelManager(self)
        self.dex = DEX(self)
        self.bridge = MultisigBridge(self)
        
        if is_genesis:
            self._genesis()
        else:
            self._load()
        
        self._register_self_miner()
        self._register_self_node()
        self._init_payment_tables()
        
        if is_genesis:
            asyncio.create_task(self.dex.create_genesis_pools())
        
        logger.info(f"[NETWORK] Initialized node: {self.node_id[:16]}... (genesis={is_genesis})")
    
    def _init_db(self):
        self.conn = sqlite3.connect('microcore.db', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        c = self.conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS miners (
            vid TEXT PRIMARY KEY, 
            pub TEXT NOT NULL, 
            username TEXT NOT NULL,
            wallet TEXT NOT NULL, 
            stake INTEGER NOT NULL, 
            level INTEGER NOT NULL,
            rewards INTEGER DEFAULT 0, 
            blocks INTEGER DEFAULT 0, 
            slashes INTEGER DEFAULT 0,
            uptime INTEGER DEFAULT 0, 
            today_uptime INTEGER DEFAULT 0, 
            type TEXT DEFAULT 'web',
            liquidity INTEGER DEFAULT 0, 
            fees INTEGER DEFAULT 0, 
            last_ping REAL DEFAULT 0,
            banned_until REAL DEFAULT 0, 
            created_at REAL DEFAULT 0, 
            last_block INTEGER DEFAULT 0,
            consecutive_misses INTEGER DEFAULT 0, 
            total_uptime INTEGER DEFAULT 0,
            best_level INTEGER DEFAULT 1, 
            ip_address TEXT DEFAULT '', 
            node_id TEXT DEFAULT '',
            version TEXT DEFAULT '', 
            active INTEGER DEFAULT 1,
            confirmed_balance INTEGER DEFAULT 0,
            password_hash TEXT DEFAULT '',
            private_key_encrypted TEXT DEFAULT ''
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY, 
            username TEXT NOT NULL, 
            wallet TEXT NOT NULL,
            ip TEXT DEFAULT '', 
            port INTEGER DEFAULT 0, 
            last_seen REAL DEFAULT 0,
            height INTEGER DEFAULT 0, 
            active INTEGER DEFAULT 1, 
            rewards_earned INTEGER DEFAULT 0,
            version TEXT DEFAULT '', 
            mining_enabled INTEGER DEFAULT 1,
            staking_enabled INTEGER DEFAULT 1, 
            last_sync REAL DEFAULT 0,
            is_multisig_signer INTEGER DEFAULT 0
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS blocks (
            id INTEGER PRIMARY KEY, 
            ts REAL NOT NULL, 
            phash TEXT NOT NULL,
            validators TEXT DEFAULT '', 
            lvl INTEGER DEFAULT 1, 
            hash TEXT NOT NULL,
            reward INTEGER DEFAULT 0, 
            tx_count INTEGER DEFAULT 0, 
            merkle_root TEXT DEFAULT '',
            nonce INTEGER DEFAULT 0, 
            difficulty INTEGER DEFAULT 1, 
            version TEXT DEFAULT '',
            challenge TEXT DEFAULT ''
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS transactions (
            tx_hash TEXT PRIMARY KEY, 
            from_wallet TEXT NOT NULL, 
            to_wallet TEXT NOT NULL,
            amount INTEGER NOT NULL, 
            fee INTEGER DEFAULT 0, 
            timestamp REAL NOT NULL,
            block_id INTEGER DEFAULT -1, 
            status TEXT DEFAULT 'pending', 
            tx_type TEXT DEFAULT 'send',
            signature TEXT DEFAULT '', 
            nonce INTEGER DEFAULT 0, 
            confirmations INTEGER DEFAULT 0,
            data TEXT DEFAULT '{}', 
            confirmed INTEGER DEFAULT 0
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS balances (wallet TEXT PRIMARY KEY, bal INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS buyer_stats (wallet TEXT PRIMARY KEY, username TEXT NOT NULL, bought REAL DEFAULT 0, last_reset REAL DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS slashing_events (id INTEGER PRIMARY KEY AUTOINCREMENT, vid TEXT NOT NULL, amount INTEGER NOT NULL, reason TEXT DEFAULT '', timestamp REAL NOT NULL, block_id INTEGER DEFAULT -1)''')
        c.execute('''CREATE TABLE IF NOT EXISTS level_supply (level INTEGER PRIMARY KEY, minted INTEGER DEFAULT 0, cap INTEGER DEFAULT 0, last_updated REAL DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pending_crypto_payments (payment_id TEXT PRIMARY KEY, wallet TEXT NOT NULL, username TEXT NOT NULL, method TEXT NOT NULL, amount INTEGER NOT NULL, usd_amount REAL NOT NULL, sender_address TEXT DEFAULT '', status TEXT DEFAULT 'pending', created_at REAL NOT NULL, completed_at REAL DEFAULT 0, txid TEXT DEFAULT '', confirmations INTEGER DEFAULT 0, error TEXT DEFAULT '')''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS tokens (symbol TEXT PRIMARY KEY, name TEXT NOT NULL, supply INTEGER NOT NULL, owner TEXT NOT NULL, created_at REAL NOT NULL, decimals INTEGER DEFAULT 18, mintable INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pools (pool_id TEXT PRIMARY KEY, token_a TEXT NOT NULL, token_b TEXT NOT NULL, reserve_a INTEGER NOT NULL, reserve_b INTEGER NOT NULL, total_lp INTEGER NOT NULL, created_at REAL NOT NULL, fee_rate REAL DEFAULT 0.003, volume_24h INTEGER DEFAULT 0, swap_count INTEGER DEFAULT 0, last_updated REAL DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS lp_positions (id INTEGER PRIMARY KEY AUTOINCREMENT, pool_id TEXT NOT NULL, wallet TEXT NOT NULL, lp_shares INTEGER NOT NULL, created_at REAL NOT NULL, last_updated REAL DEFAULT 0, fees_earned INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS dex_swaps (id INTEGER PRIMARY KEY AUTOINCREMENT, swap_hash TEXT UNIQUE NOT NULL, pool_id TEXT NOT NULL, user TEXT NOT NULL, token_in TEXT NOT NULL, token_out TEXT NOT NULL, amount_in INTEGER NOT NULL, amount_out INTEGER NOT NULL, fee INTEGER NOT NULL, gas_fee INTEGER DEFAULT 0, price REAL DEFAULT 0, timestamp REAL NOT NULL, block_id INTEGER DEFAULT -1, status TEXT DEFAULT 'completed')''')
        c.execute('''CREATE TABLE IF NOT EXISTS dex_tx (id INTEGER PRIMARY KEY AUTOINCREMENT, tx_hash TEXT UNIQUE NOT NULL, pool_id TEXT NOT NULL, wallet TEXT NOT NULL, tx_type TEXT NOT NULL, amount_a INTEGER DEFAULT 0, amount_b INTEGER DEFAULT 0, lp_shares INTEGER DEFAULT 0, gas_fee INTEGER DEFAULT 0, timestamp REAL NOT NULL, block_id INTEGER DEFAULT -1, status TEXT DEFAULT 'completed')''')
        c.execute('''CREATE TABLE IF NOT EXISTS price_history (id INTEGER PRIMARY KEY AUTOINCREMENT, pool_id TEXT NOT NULL, price REAL NOT NULL, timestamp REAL NOT NULL, volume INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS token_balances (wallet TEXT NOT NULL, token_symbol TEXT NOT NULL, balance INTEGER NOT NULL, PRIMARY KEY (wallet, token_symbol))''')
        c.execute('''CREATE TABLE IF NOT EXISTS duco_transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, txid TEXT UNIQUE NOT NULL, wallet TEXT NOT NULL, username TEXT NOT NULL, amount REAL NOT NULL, mcx_amount INTEGER NOT NULL, tx_time REAL NOT NULL, verified_at REAL DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS towers (id INTEGER PRIMARY KEY AUTOINCREMENT, wallet TEXT NOT NULL, level INTEGER NOT NULL, amount INTEGER NOT NULL, type TEXT DEFAULT 'temporary', created_at REAL DEFAULT 0, expires_at REAL DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS swaps (id TEXT PRIMARY KEY, from_wallet TEXT NOT NULL, to_wallet TEXT NOT NULL, from_token TEXT NOT NULL, to_token TEXT NOT NULL, amount_in REAL NOT NULL, amount_out REAL DEFAULT 0, fee REAL DEFAULT 0, gas_fee INTEGER DEFAULT 0, status TEXT DEFAULT 'pending', created_at REAL DEFAULT 0, completed_at REAL DEFAULT 0, tx_hash TEXT DEFAULT '')''')
        c.execute('''CREATE TABLE IF NOT EXISTS validator_history (id INTEGER PRIMARY KEY AUTOINCREMENT, vid TEXT NOT NULL, username TEXT NOT NULL, block_id INTEGER NOT NULL, level INTEGER NOT NULL, timestamp REAL NOT NULL, reward INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS node_wallet (address TEXT PRIMARY KEY, private_key_encrypted TEXT NOT NULL, public_key TEXT NOT NULL, created_at REAL NOT NULL, last_updated REAL DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS real_token_bridges (token_symbol TEXT PRIMARY KEY, bridge_address TEXT NOT NULL, balance REAL DEFAULT 0, last_updated REAL DEFAULT 0, private_key_encrypted TEXT DEFAULT '', multisig_threshold INTEGER DEFAULT 5, total_signers INTEGER DEFAULT 10)''')
        c.execute('''CREATE TABLE IF NOT EXISTS bridge_swap_requests (request_id TEXT PRIMARY KEY, user_wallet TEXT NOT NULL, token_from TEXT NOT NULL, token_to TEXT NOT NULL, amount_from REAL NOT NULL, amount_to REAL NOT NULL, status TEXT DEFAULT 'pending', created_at REAL NOT NULL, completed_at REAL DEFAULT 0, tx_hash TEXT DEFAULT '', real_tx_hash TEXT DEFAULT '', error TEXT DEFAULT '')''')
        c.execute('''CREATE TABLE IF NOT EXISTS multisig_proposals (proposal_id TEXT PRIMARY KEY, token_symbol TEXT NOT NULL, action TEXT NOT NULL, amount REAL NOT NULL, recipient_wallet TEXT NOT NULL, signatures TEXT DEFAULT '[]', status TEXT DEFAULT 'pending', created_at REAL NOT NULL, tx_hash TEXT DEFAULT '', memo TEXT DEFAULT '')''')
        c.execute('''CREATE TABLE IF NOT EXISTS duco_payments (txid TEXT PRIMARY KEY, wallet TEXT NOT NULL, username TEXT NOT NULL, amount REAL NOT NULL, memo TEXT NOT NULL, timestamp REAL NOT NULL, verified INTEGER DEFAULT 0, mcx_credited INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS duco_pool (id INTEGER PRIMARY KEY AUTOINCREMENT, wallet TEXT NOT NULL, amount REAL NOT NULL, action TEXT NOT NULL, timestamp REAL NOT NULL, tx_hash TEXT DEFAULT '')''')
        c.execute('''CREATE TABLE IF NOT EXISTS genesis_wallet (id INTEGER PRIMARY KEY AUTOINCREMENT, wallet TEXT NOT NULL, username TEXT NOT NULL, created_at REAL NOT NULL, password_hash TEXT NOT NULL)''')
        
        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(from_wallet, to_wallet)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_transactions_block ON transactions(block_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_miners_username ON miners(username)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_miners_active ON miners(active)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON pending_crypto_payments(status)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_lp_wallet ON lp_positions(wallet)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_swaps_wallet ON swaps(from_wallet, to_wallet)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pools_tokens ON pools(token_a, token_b)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_price_history_pool ON price_history(pool_id, timestamp)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_bridge_swaps_user ON bridge_swap_requests(user_wallet)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_bridge_swaps_status ON bridge_swap_requests(status)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_multisig_proposals_status ON multisig_proposals(status)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_duco_pool_wallet ON duco_pool(wallet)")
        except sqlite3.OperationalError as e:
            logger.warning(f"[DB] Index warning: {e}")
        
        for level in range(1, 11):
            try:
                c.execute("INSERT OR IGNORE INTO level_supply (level, minted, cap, last_updated) VALUES (?, 0, ?, ?)", 
                         (level, LEVEL_CAPS[level], time.time()))
            except:
                pass
        
        self.conn.commit()
        logger.info("[DB] Database initialized")
    
    def _init_payment_tables(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS pending_crypto_payments (payment_id TEXT PRIMARY KEY, wallet TEXT NOT NULL, username TEXT NOT NULL, method TEXT NOT NULL, amount INTEGER NOT NULL, usd_amount REAL NOT NULL, sender_address TEXT DEFAULT '', status TEXT DEFAULT 'pending', created_at REAL NOT NULL, completed_at REAL DEFAULT 0, txid TEXT DEFAULT '', confirmations INTEGER DEFAULT 0, error TEXT DEFAULT '')''')
        self.conn.commit()
    
    def _save_balance(self, wallet: str, balance: int):
        self.conn.execute("INSERT OR REPLACE INTO balances VALUES (?, ?)", (wallet, balance))
        self.conn.commit()
    
    def _save_transaction(self, tx: Transaction):
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (tx.tx_hash, tx.from_wallet, tx.to_wallet, tx.amount, tx.fee, tx.timestamp, tx.block_id, tx.status, tx.tx_type, tx.signature, tx.nonce, tx.confirmations, json.dumps(tx.data), 1 if tx.confirmed else 0))
        self.conn.commit()
    
    def _save_miner(self, miner: Miner):
        """Save miner to database - COMPLETE FIX"""
        c = self.conn.cursor()
        
        private_key_str = miner.private_key_encrypted
        if isinstance(private_key_str, dict):
            private_key_str = json.dumps(private_key_str)
        elif private_key_str is None:
            private_key_str = ""
        elif not isinstance(private_key_str, str):
            private_key_str = str(private_key_str)
        
        password_str = miner.password_hash
        if isinstance(password_str, dict):
            password_str = json.dumps(password_str)
        elif password_str is None:
            password_str = ""
        elif not isinstance(password_str, str):
            password_str = str(password_str)
        
        try:
            c.execute("""INSERT OR REPLACE INTO miners VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(miner.vid),
                    str(miner.pub),
                    str(miner.username),
                    str(miner.wallet),
                    int(miner.stake),
                    int(miner.level),
                    int(miner.rewards),
                    int(miner.blocks),
                    int(miner.slashes),
                    int(miner.uptime),
                    int(miner.today_uptime),
                    str(miner.mtype),
                    int(miner.liquidity_provided),
                    int(miner.fees_collected),
                    float(miner.last_ping),
                    float(miner.banned_until),
                    float(miner.created_at),
                    int(miner.last_block),
                    int(miner.consecutive_misses),
                    int(miner.total_uptime),
                    int(miner.best_level),
                    str(miner.ip_address),
                    str(miner.node_id),
                    str(miner.version),
                    1 if miner.active else 0,
                    int(miner.confirmed_balance),
                    password_str,
                    private_key_str
                ))
            self.conn.commit()
            logger.debug(f"[DB] Miner {miner.username} saved successfully")
        except Exception as e:
            logger.error(f"[DB] Failed to save miner: {e}")
            raise
    
    def _save_node(self, node: Node):
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (node.node_id, node.username, node.wallet, node.ip, node.port, node.last_seen, node.height, 1 if node.active else 0, node.rewards_earned, node.version, 1 if node.mining_enabled else 0, 1 if node.staking_enabled else 0, node.last_sync, 1 if node.is_multisig_signer else 0))
        self.conn.commit()
    
    def _genesis(self):
        if self.conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0] == 0:
            self.balances[self.wallet] = GENESIS_SUPPLY
            self.total_minted = GENESIS_SUPPLY
            self._save_balance(self.wallet, GENESIS_SUPPLY)
            
            c = self.conn.cursor()
            c.execute("INSERT OR REPLACE INTO genesis_wallet (wallet, username, created_at, password_hash) VALUES (?, ?, ?, ?)",
                     (self.wallet, self.display_name, time.time(), sha256_hash(self.password) if self.password else ""))
            self.conn.commit()
            
            logger.info(f"[GENESIS] Created genesis wallet:")
            logger.info(f"  Username: {self.display_name}")
            logger.info(f"  Wallet: {self.wallet}")
            logger.info(f"  Balance: {GENESIS_SUPPLY:,} MCX (NOT STAKED)")
            logger.info(f"  Password: {'SET' if self.password else 'NOT SET'}")
            
            self._add_block(0, "0"*64, ["genesis"], 1, {}, "genesis")
            for level in range(1, 11):
                c.execute("UPDATE level_supply SET minted=0 WHERE level=?", (level,))
            self.conn.commit()
            logger.info("[GENESIS] Genesis block created")
    
    def _load(self):
        for row in self.conn.execute("SELECT wallet, bal FROM balances"):
            self.balances[row[0]] = row[1]
        logger.info(f"[LOAD] Loaded {len(self.balances)} balances")
        
        for row in self.conn.execute("SELECT * FROM blocks ORDER BY id"):
            validators = row[3].split(',') if row[3] else []
            block = Block(row[0], row[1], row[2], validators, row[4], {}, row[5], row[6], row[7])
            block.merkle_root = row[8] if len(row) > 8 else ""
            block.nonce = row[9] if len(row) > 9 else 0
            block.difficulty = row[10] if len(row) > 10 else 1
            block.challenge = row[11] if len(row) > 11 else ""
            self.blocks.append(block)
            if block.id >= self.height:
                self.height = block.id + 1
                self.last_hash = block.hash
        logger.info(f"[LOAD] Loaded {len(self.blocks)} blocks")
        
        for row in self.conn.execute("SELECT * FROM miners"):
            miner = Miner(
                vid=row[0], pub=row[1], username=row[2], wallet=row[3],
                stake=row[4], level=row[5], rewards=row[6], blocks=row[7],
                slashes=row[8], uptime=row[9], today_uptime=row[10],
                mtype=row[11], liquidity_provided=row[12], fees_collected=row[13],
                last_ping=row[14], banned_until=row[15], created_at=row[16],
                last_block=row[17], consecutive_misses=row[18], total_uptime=row[19],
                best_level=row[20], ip_address=row[21] or "", node_id=row[22] or "",
                version=row[23] or VERSION, active=bool(row[24]),
                confirmed_balance=row[25] if len(row) > 25 else 0,
                password_hash=row[26] if len(row) > 26 else "",
                private_key_encrypted=row[27] if len(row) > 27 else ""
            )
            self.miners[row[0]] = miner
        logger.info(f"[LOAD] Loaded {len(self.miners)} miners")
        
        for row in self.conn.execute("SELECT * FROM nodes"):
            node = Node(row[0], row[1], row[2], row[3] or "", row[4] or 0, row[5] or time.time(), row[6] or 0, bool(row[7]), row[8] or 0, row[9] or VERSION, [], bool(row[10]) if len(row) > 10 else True, bool(row[11]) if len(row) > 11 else True, row[12] or 0, bool(row[13]) if len(row) > 13 else False)
            self.nodes[row[0]] = node
        logger.info(f"[LOAD] Loaded {len(self.nodes)} nodes")
        
        for row in self.conn.execute("SELECT * FROM transactions WHERE block_id = -1 AND status = 'pending'"):
            tx = Transaction(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], json.loads(row[12]) if row[12] else {})
            tx.confirmed = bool(row[13]) if len(row) > 13 else False
            asyncio.create_task(self.mempool.add_transaction(tx))
        logger.info(f"[LOAD] Loaded {self.mempool.size()} mempool transactions")
        
        c = self.conn.cursor()
        c.execute("SELECT wallet, level, amount, type FROM towers WHERE expires_at > ? OR type = 'permanent'", (time.time(),))
        for row in c.fetchall():
            wallet, level, amount, tower_type = row
            if tower_type == "temporary":
                if wallet not in self.level_mgr.temp_towers:
                    self.level_mgr.temp_towers[wallet] = {}
                self.level_mgr.temp_towers[wallet][level] = amount
            else:
                self.level_mgr.perm_towers[wallet] = self.level_mgr.perm_towers.get(wallet, 0) + amount
        logger.info("[LOAD] State loaded successfully")
    
    def _register_self_miner(self):
        for m in self.miners.values():
            if m.wallet == self.wallet:
                logger.info(f"[SELF MINER] Wallet {self.wallet[:16]}... already registered as {m.username}")
                m.last_ping = time.time()
                m.active = True
                m.confirmed_balance = self.balances.get(self.wallet, 0)
                m.password_hash = sha256_hash(self.password) if self.password else ""
                m.private_key_encrypted = encrypt_private_key(self.priv, self.password) if self.password else ""
                self._save_miner(m)
                return
        
        private_key_encrypted = ""
        if self.password:
            encrypted = encrypt_private_key(self.priv, self.password)
            private_key_encrypted = json.dumps(encrypted)
        
        miner = Miner(
            vid=self.wallet,
            pub=self.pub,
            username=self.display_name,
            wallet=self.wallet,
            stake=0,
            level=1,
            uptime=0,
            today_uptime=0,
            last_ping=time.time(),
            active=True,
            rewards=0,
            blocks=0,
            slashes=0,
            misses=0,
            mtype="pc",
            created_at=time.time(),
            version=VERSION,
            confirmed_balance=self.balances.get(self.wallet, 0),
            password_hash=sha256_hash(self.password) if self.password else "",
            private_key_encrypted=private_key_encrypted
        )
        self.miners[self.wallet] = miner
        self._save_miner(miner)
        logger.info(f"[SELF MINER] Registered wallet {self.wallet[:16]}... as {self.display_name} with {miner.confirmed_balance} MCX (NOT STAKED)")
    
    def _register_self_node(self):
        if self.node_id not in self.nodes:
            node = Node(
                node_id=self.node_id,
                username=self.display_name,
                wallet=self.wallet,
                ip="127.0.0.1",
                port=NODE_PORT,
                last_seen=time.time(),
                height=self.height,
                active=True,
                rewards_earned=0,
                version=VERSION,
                peers=[],
                mining_enabled=True,
                staking_enabled=True,
                last_sync=time.time(),
                is_multisig_signer=True
            )
            self.nodes[self.node_id] = node
            self._save_node(node)
            logger.info(f"[SELF NODE] Registered node ID: {self.node_id[:16]}...")
    
    def login(self, wallet: str, password: str) -> bool:
        for m in self.miners.values():
            if m.wallet == wallet:
                if m.password_hash == sha256_hash(password):
                    return True
        return False
    
    def get_private_key(self, wallet: str, password: str) -> Optional[str]:
        for m in self.miners.values():
            if m.wallet == wallet:
                if m.password_hash == sha256_hash(password) and m.private_key_encrypted:
                    try:
                        return decrypt_private_key(json.loads(m.private_key_encrypted), password)
                    except:
                        return None
                return None
        return None
    
    # ==================== REGISTRATION (COMPLETE FIX) ====================
    def register_miner(self, vid: str, pub: str, username: str, wallet: str, stake: int, sig: str, timestamp: int, mtype: str = "web") -> bool:
        logger.info(f"[REGISTER] ==================================")
        logger.info(f"[REGISTER] username={username}")
        logger.info(f"[REGISTER] wallet={wallet[:16]}...")
        logger.info(f"[REGISTER] type={mtype}")
        logger.info(f"[REGISTER] stake={stake}")
        logger.info(f"[REGISTER] vid={vid[:16]}...")
        
        # Check if wallet already exists
        for existing_miner in self.miners.values():
            if existing_miner.wallet == wallet:
                logger.info(f"[REGISTER] ⚠️ Wallet {wallet[:16]}... already registered as {existing_miner.username}")
                existing_miner.last_ping = time.time()
                existing_miner.active = True
                existing_miner.stake = max(existing_miner.stake, stake)
                self.level_mgr.register(wallet, existing_miner.stake)
                existing_miner.level = self.level_mgr.get_level(wallet)
                self._save_miner(existing_miner)
                return True
        
        # Check if vid already exists
        if vid in self.miners:
            logger.info(f"[REGISTER] ⚠️ VID {vid[:16]}... already registered")
            self.miners[vid].last_ping = time.time()
            self.miners[vid].active = True
            self._save_miner(self.miners[vid])
            return True
        
        # Check if banned
        c = self.conn.cursor()
        c.execute("SELECT banned_until FROM miners WHERE vid=?", (vid,))
        row = c.fetchone()
        if row and row[0] and row[0] > time.time():
            logger.warning(f"[REGISTER] ❌ {username} is banned until {row[0]}")
            return False
        
        # Verify signature
        msg = f"{username}{wallet}{timestamp}"
        logger.info(f"[REGISTER] Verifying signature for {username}...")
        
        if not verify_signature(pub, msg, sig, mtype):
            logger.warning(f"[REGISTER] ❌ INVALID signature from {username} (type: {mtype})")
            return False
        
        logger.info(f"[REGISTER] ✅ Signature verified for {username}")
        
        # Get confirmed balance
        confirmed_bal = self.balances.get(wallet, 0)
        logger.info(f"[REGISTER] Confirmed balance for {wallet[:16]}...: {confirmed_bal}")
        
        # Create and save miner
        miner = Miner(
            vid=vid,
            pub=pub,
            username=username,
            wallet=wallet,
            stake=stake,
            level=1,
            uptime=0,
            today_uptime=0,
            last_ping=time.time(),
            active=True,
            rewards=0,
            blocks=0,
            slashes=0,
            misses=0,
            mtype=mtype,
            created_at=time.time(),
            version=VERSION,
            confirmed_balance=confirmed_bal,
            password_hash="",
            private_key_encrypted=""
        )
        
        self.miners[vid] = miner
        self.miners[wallet] = miner
        
        self.level_mgr.register(wallet, stake)
        miner.level = self.level_mgr.get_level(wallet)
        
        self._save_miner(miner)
        
        c.execute("SELECT COUNT(*) FROM miners WHERE vid=?", (vid,))
        count = c.fetchone()[0]
        if count > 0:
            logger.info(f"[REGISTER] ✅ MINER SAVED! {username} (vid: {vid[:16]}...)")
        else:
            logger.error(f"[REGISTER] ❌ MINER NOT SAVED! {username}")
            return False
        
        logger.info(f"[REGISTER] ✅ {username} (type: {mtype}) registered with {confirmed_bal} MCX, Level {miner.level}")
        return True
    
    def update_miner_uptime(self, vid: str, uptime_seconds: int, today_uptime: int):
        if vid in self.miners:
            miner = self.miners[vid]
            miner.uptime = uptime_seconds
            miner.today_uptime = today_uptime
            miner.last_ping = time.time()
            miner.active = True
            miner.total_uptime += 1
            self._save_miner(miner)
    
    def cleanup_inactive_miners(self) -> int:
        timeout = time.time() - 900
        c = self.conn.cursor()
        c.execute("SELECT vid, username FROM miners WHERE last_ping < ? AND active = 1", (timeout,))
        inactive = c.fetchall()
        for vid, username in inactive:
            if vid in self.miners:
                self.miners[vid].active = False
                self._save_miner(self.miners[vid])
        self.conn.commit()
        return len(inactive)
    
    def get_miners_list(self) -> List[dict]:
        return [{"vid": m.vid, "username": m.username, "wallet": m.wallet, "level": m.level, "stake": m.stake, "blocks": m.blocks, "rewards": m.rewards, "active": m.active, "uptime": m.uptime, "today_uptime": m.today_uptime, "type": m.mtype, "last_seen": m.last_ping, "confirmed_balance": m.confirmed_balance} for m in self.miners.values()]
    
    def get_nodes_list(self) -> List[dict]:
        return [{"node_id": n.node_id, "username": n.username, "wallet": n.wallet, "ip": n.ip, "port": n.port, "height": n.height, "active": n.active, "rewards": n.rewards_earned, "version": n.version, "is_multisig_signer": n.is_multisig_signer} for n in self.nodes.values()]
    
    # ==================== STAKE FUNCTIONS ====================
    def process_stake(self, wallet: str, amount: int, signature: str, timestamp: int) -> dict:
        miner = None
        for m in self.miners.values():
            if m.wallet == wallet:
                miner = m
                break
        
        if not miner:
            return {"success": False, "error": "Wallet not found"}
        
        msg = f"{miner.username}stake{amount}{timestamp}"
        if not verify_signature(miner.pub, msg, signature, "pc"):
            return {"success": False, "error": "Invalid signature"}
        
        if miner.confirmed_balance < amount:
            return {"success": False, "error": f"Insufficient confirmed balance. You have {miner.confirmed_balance} MCX available"}
        
        miner.confirmed_balance -= amount
        miner.stake += amount
        self.balances[wallet] = miner.confirmed_balance
        
        self.level_mgr.register(wallet, miner.stake)
        miner.level = self.level_mgr.get_level(wallet)
        self._save_miner(miner)
        self._save_balance(wallet, miner.confirmed_balance)
        
        tx_hash = hash_transaction({"from": wallet, "to": "stake_pool", "amount": amount})
        tx = Transaction(tx_hash, wallet, "stake_pool", amount, 0, time.time(), -1, "confirmed", "stake")
        self._save_transaction(tx)
        
        return {"success": True, "staked": miner.stake, "level": miner.level, "confirmed_balance": miner.confirmed_balance}
    
    def process_unstake(self, wallet: str, amount: int, signature: str, timestamp: int) -> dict:
        miner = None
        for m in self.miners.values():
            if m.wallet == wallet:
                miner = m
                break
        
        if not miner:
            return {"success": False, "error": "Wallet not found"}
        
        msg = f"{miner.username}unstake{amount}{timestamp}"
        if not verify_signature(miner.pub, msg, signature, "pc"):
            return {"success": False, "error": "Invalid signature"}
        
        if miner.stake < amount:
            return {"success": False, "error": f"Insufficient staked. You have {miner.stake} MCX staked"}
        
        miner.stake -= amount
        miner.confirmed_balance += amount
        self.balances[wallet] = miner.confirmed_balance
        
        self.level_mgr.register(wallet, miner.stake)
        miner.level = self.level_mgr.get_level(wallet)
        self._save_miner(miner)
        self._save_balance(wallet, miner.confirmed_balance)
        
        tx_hash = hash_transaction({"from": "stake_pool", "to": wallet, "amount": amount})
        tx = Transaction(tx_hash, "stake_pool", wallet, amount, 0, time.time(), -1, "confirmed", "unstake")
        self._save_transaction(tx)
        
        return {"success": True, "staked": miner.stake, "level": miner.level, "confirmed_balance": miner.confirmed_balance}
    
    def get_balance(self, wallet: str) -> int:
        return self.balances.get(wallet, 0)
    
    def get_confirmed_balance(self, wallet: str) -> int:
        for m in self.miners.values():
            if m.wallet == wallet:
                return m.confirmed_balance
        return self.balances.get(wallet, 0)
    
    def get_top_stakers(self, limit: int = 10) -> List[dict]:
        stakers = []
        for m in self.miners.values():
            if m.active and m.stake > 0:
                stakers.append({"username": m.username, "wallet": m.wallet, "staked": m.stake, "level": m.level})
        stakers.sort(key=lambda x: x["staked"], reverse=True)
        return stakers[:limit]
    
    def get_buyer_stats(self, limit: int = 10) -> List[dict]:
        c = self.conn.cursor()
        c.execute("SELECT wallet, username, bought FROM buyer_stats ORDER BY bought DESC LIMIT ?", (limit,))
        return [{"wallet": r[0], "username": r[1], "bought": r[2]} for r in c.fetchall()]
    
    # ==================== SEND MCX ====================
    def send_mcx(self, from_wallet: str, to_wallet: str, amount: int, signature: str, timestamp: int) -> dict:
        from_miner = None
        for m in self.miners.values():
            if m.wallet == from_wallet:
                from_miner = m
                break
        
        if not from_miner:
            return {"success": False, "error": f"Sender wallet '{from_wallet[:16]}...' not found"}
        
        to_miner = None
        for m in self.miners.values():
            if m.wallet == to_wallet:
                to_miner = m
                break
        
        if not to_miner:
            return {"success": False, "error": f"Recipient wallet '{to_wallet[:16]}...' not found"}
        
        msg = f"{from_miner.username}{to_miner.username}{amount}{timestamp}"
        if not verify_signature(from_miner.pub, msg, signature, "pc"):
            return {"success": False, "error": "Invalid signature"}
        
        fee = calculate_transfer_fee(amount)
        fee = int(fee)
        if fee < 1:
            fee = 1
        
        if from_miner.confirmed_balance < amount + fee:
            return {"success": False, "error": f"Insufficient confirmed balance. You have {from_miner.confirmed_balance} MCX available"}
        
        from_miner.confirmed_balance -= (amount + fee)
        to_miner.confirmed_balance += amount
        
        self.balances[from_wallet] = from_miner.confirmed_balance
        self.balances[to_wallet] = to_miner.confirmed_balance
        
        self.validator_fee_pool += fee
        self._save_miner(from_miner)
        self._save_miner(to_miner)
        self._save_balance(from_wallet, from_miner.confirmed_balance)
        self._save_balance(to_wallet, to_miner.confirmed_balance)
        
        tx_hash = hash_transaction({"from": from_wallet, "to": to_wallet, "amount": amount, "fee": fee})
        tx = Transaction(tx_hash, from_wallet, to_wallet, amount, fee, time.time(), -1, "confirmed", "send", signature)
        tx.confirmed = True
        self._save_transaction(tx)
        
        return {"success": True, "tx_hash": tx_hash, "from": from_wallet, "to": to_wallet, "amount": amount, "fee": fee}
    
    def get_transactions(self, wallet: str, limit: int = 20) -> List[dict]:
        c = self.conn.cursor()
        c.execute("SELECT tx_hash, from_wallet, to_wallet, amount, fee, timestamp, block_id, status, tx_type FROM transactions WHERE from_wallet = ? OR to_wallet = ? ORDER BY timestamp DESC LIMIT ?", (wallet, wallet, limit))
        return [{"hash": r[0], "from": r[1], "to": r[2], "amount": r[3], "fee": r[4], "timestamp": r[5], "block": r[6], "status": r[7], "type": r[8]} for r in c.fetchall()]
    
    # ==================== BLOCK FUNCTIONS ====================
    def get_blocks_range(self, start: int, end: int) -> List[dict]:
        blocks = []
        for b in self.blocks:
            if start <= b.id <= end:
                blocks.append({"id": b.id, "ts": b.ts, "prev": b.prev, "validators": b.validators, "level": b.level, "hash": b.hash, "reward": b.reward, "tx_count": b.tx_count, "challenge": b.challenge})
        return blocks
    
    def _add_block(self, bid: int, prev: str, validators: List[str], level: int, sigs: dict, challenge: str) -> Block:
        ts = time.time()
        block = Block(bid, ts, prev, validators, level, sigs, "", 0)
        block.challenge = challenge
        block.hash = hash_block({"id": bid, "ts": ts, "prev": prev, "validators": validators, "level": level, "merkle_root": block.merkle_root, "nonce": block.nonce, "challenge": challenge})
        self.blocks.append(block)
        self.height = bid + 1
        self.last_hash = block.hash
        self.conn.execute("INSERT INTO blocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                         (bid, ts, prev, ','.join(validators), level, block.hash, 0, 0, block.merkle_root, block.nonce, block.difficulty, block.version, challenge))
        self.conn.commit()
        return block
    
    async def import_blocks(self, blocks_data: List[dict]):
        for b in sorted(blocks_data, key=lambda x: x['id']):
            if b['id'] >= self.height and b['prev'] == self.last_hash:
                valid_sigs = 0
                for vid, sig in b.get('sigs', {}).items():
                    miner = self.miners.get(vid)
                    if not miner:
                        continue
                    msg = f"{b['challenge']}{vid}{b['id']}"
                    if verify_signature(miner.pub, msg, sig, miner.mtype):
                        valid_sigs += 1
                
                if valid_sigs < MIN_VALIDATORS_PER_BLOCK:
                    continue
                
                block_hash = hash_block({"id": b['id'], "ts": b['ts'], "prev": b['prev'], 
                                        "validators": b['validators'], "level": b['level'],
                                        "merkle_root": b.get('merkle_root', ''), 
                                        "nonce": b.get('nonce', 0),
                                        "challenge": b.get('challenge', '')})
                if block_hash != b['hash']:
                    continue
                
                block = Block(b['id'], b['ts'], b['prev'], b['validators'], b['level'], b.get('sigs', {}), b['hash'], b.get('reward', 0))
                block.challenge = b.get('challenge', '')
                self.blocks.append(block)
                self.height = b['id'] + 1
                self.last_hash = block.hash
                self.conn.execute("INSERT INTO blocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                                 (b['id'], b['ts'], b['prev'], ','.join(b['validators']), b['level'], b['hash'], b.get('reward', 0), 0, "", 0, 1, VERSION, b.get('challenge', '')))
                self.conn.commit()
                logger.info(f"[SYNC] Imported block {b['id']}")
    
    async def receive_block(self, block_data: dict):
        bid = block_data.get('id')
        if bid == self.height and block_data.get('prev') == self.last_hash:
            valid_sigs = 0
            for vid, sig in block_data.get('sigs', {}).items():
                miner = self.miners.get(vid)
                if not miner:
                    continue
                msg = f"{block_data['challenge']}{vid}{bid}"
                if verify_signature(miner.pub, msg, sig, miner.mtype):
                    valid_sigs += 1
            
            if valid_sigs < MIN_VALIDATORS_PER_BLOCK:
                return
            
            block_hash = hash_block({"id": bid, "ts": block_data['ts'], "prev": block_data['prev'], 
                                    "validators": block_data['validators'], "level": block_data['level'],
                                    "merkle_root": block_data.get('merkle_root', ''), 
                                    "nonce": block_data.get('nonce', 0),
                                    "challenge": block_data.get('challenge', '')})
            if block_hash != block_data['hash']:
                return
            
            block = Block(bid, block_data['ts'], block_data['prev'], block_data['validators'], block_data['level'], block_data.get('sigs', {}), block_data['hash'], block_data.get('reward', 0))
            block.challenge = block_data.get('challenge', '')
            self.blocks.append(block)
            self.height = bid + 1
            self.last_hash = block.hash
            self.conn.execute("INSERT INTO blocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                             (bid, block.ts, block.prev, ','.join(block.validators), block.level, block.hash, block.reward, 0, "", 0, 1, VERSION, block.challenge))
            self.conn.commit()
            logger.info(f"[P2P] Received block {bid}")
    
    async def receive_transaction(self, tx_data: dict):
        tx_hash = tx_data.get('hash')
        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM transactions WHERE tx_hash=?", (tx_hash,))
        if c.fetchone()[0] == 0:
            from_wallet = tx_data.get('from')
            signature = tx_data.get('signature', '')
            msg = f"{tx_data['from']}{tx_data['to']}{tx_data['amount']}{tx_data['timestamp']}"
            
            pub = None
            for m in self.miners.values():
                if m.wallet == from_wallet:
                    pub = m.pub
                    break
            
            if pub and verify_signature(pub, msg, signature, "pc"):
                tx = Transaction(tx_hash, tx_data['from'], tx_data['to'], tx_data['amount'], tx_data.get('fee', 1), tx_data['timestamp'], -1, 'pending', tx_data.get('type', 'send'), signature)
                self._save_transaction(tx)
                await self.mempool.add_transaction(tx)

    # ==================== LEVEL FUNCTIONS ====================
    def get_block_interval(self, level: int) -> int:
        return LEVEL_BLOCK_INTERVALS.get(level, 40)
    
    def get_current_reward_for_level(self, level: int) -> int:
        c = self.conn.cursor()
        c.execute("SELECT minted, cap FROM level_supply WHERE level=?", (level,))
        row = c.fetchone()
        if not row:
            return INITIAL_BLOCK_REWARD
        minted, cap = row
        remaining = cap - minted
        if remaining <= 0:
            return 0
        halving_count = 0
        target = cap / 2
        while minted >= target and target > 0:
            halving_count += 1
            target = cap / (2 ** (halving_count + 1))
        reward = INITIAL_BLOCK_REWARD // (2 ** halving_count)
        return max(reward, 1)
    
    def update_level_supply(self, level: int, reward: int):
        c = self.conn.cursor()
        c.execute("UPDATE level_supply SET minted = minted + ?, last_updated = ? WHERE level = ?", (reward, time.time(), level))
        self.conn.commit()
    
    def get_total_minted(self) -> int:
        c = self.conn.cursor()
        c.execute("SELECT SUM(minted) FROM level_supply")
        total = c.fetchone()[0]
        return total or 0
    
    def get_remaining_supply_for_level(self, level: int) -> int:
        c = self.conn.cursor()
        c.execute("SELECT minted, cap FROM level_supply WHERE level=?", (level,))
        row = c.fetchone()
        if row:
            return max(0, row[1] - row[0])
        return LEVEL_CAPS.get(level, 0)
    
    def update_level_groups(self):
        self.level_groups = {i: [] for i in range(1, 11)}
        self.levels_with_miners = set()
        for miner in self.miners.values():
            if miner.active:
                level = min(miner.level, MAX_LEVEL)
                if level in self.level_groups:
                    self.level_groups[level].append(miner.vid)
                    self.levels_with_miners.add(level)
    
    def get_level_with_most_miners(self) -> Optional[int]:
        best_level = 1
        best_count = 0
        for level in range(1, 11):
            count = len(self.level_groups.get(level, []))
            if count > best_count:
                best_count = count
                best_level = level
        return best_level if best_count >= MIN_VALIDATORS_PER_BLOCK else None
    
    def get_effective_production_level(self, original_level: int) -> Optional[int]:
        miners_count = len(self.level_groups.get(original_level, []))
        if miners_count >= MIN_VALIDATORS_PER_BLOCK:
            return original_level
        else:
            return self.get_level_with_most_miners()
    
    # ==================== SLASH FUNCTIONS ====================
    def slash_miner(self, vid: str, reason: str, block_id: int = -1) -> int:
        if vid not in self.miners:
            return 0
        m = self.miners[vid]
        slash = max(int(m.stake * SLASH_RATE), LEVEL_STAKE_RANGE)
        m.stake -= slash
        if m.stake < LEVEL_STAKE_RANGE:
            m.stake = LEVEL_STAKE_RANGE
        m.slashes += 1
        m.misses += 1
        m.consecutive_misses += 1
        self.level_mgr.register(m.wallet, m.stake)
        m.level = self.level_mgr.get_level(m.wallet)
        if m.slashes >= BAN_THRESHOLD:
            m.active = False
            m.banned_until = time.time() + BAN_DURATION
        self._save_miner(m)
        self.conn.execute("INSERT INTO slashing_events (vid, amount, reason, timestamp, block_id) VALUES (?, ?, ?, ?, ?)", (vid, slash, reason, time.time(), block_id))
        self.conn.commit()
        self.metrics["slash_events"] += 1
        asyncio.create_task(self.p2p.broadcast_slash(vid))
        return slash
    
    # ==================== CONSENSUS FUNCTIONS ====================
    def select_validators_for_level(self, level: int) -> List[str]:
        miners = self.level_groups.get(level, [])
        if len(miners) < MIN_VALIDATORS_PER_BLOCK:
            return []
        
        seed = int(self.last_hash[:16], 16) if self.last_hash != "0"*64 else int(time.time())
        rng = random.Random(seed)
        
        weighted_miners = []
        for vid in miners:
            if vid in self.miners:
                m = self.miners[vid]
                weight = m.stake // 100
                weighted_miners.extend([vid] * max(1, weight))
        
        if not weighted_miners:
            return rng.sample(miners, min(len(miners), MIN_VALIDATORS_PER_BLOCK))
        
        selected = []
        available = weighted_miners.copy()
        while len(selected) < MIN_VALIDATORS_PER_BLOCK and available:
            choice = rng.choice(available)
            if choice not in selected:
                selected.append(choice)
            available = [v for v in available if v != choice]
        
        return selected
    
    def generate_challenge(self, block_id: int, validators: List[str]) -> str:
        return hashlib.sha256(f"{block_id}{''.join(sorted(validators))}{time.time()}{self.last_hash}{secrets.token_hex(16)}".encode()).hexdigest()
    
    def verify_challenge_response(self, vid: str, challenge: str, block_id: int, sig: str) -> bool:
        if vid not in self.miners:
            return False
        message = f"{challenge}{vid}{block_id}"
        return verify_signature(self.miners[vid].pub, message, sig, self.miners[vid].mtype)
    
    # ==================== BLOCK PRODUCTION ====================
    async def produce_block_for_level(self, original_level: int) -> bool:
        effective_level = self.get_effective_production_level(original_level)
        if effective_level is None:
            return False
        
        validators = self.select_validators_for_level(effective_level)
        if len(validators) < MIN_VALIDATORS_PER_BLOCK:
            return False
        
        block_id = self.height        
        challenge = self.generate_challenge(block_id, validators)
        self.pending_challenges[challenge] = {
            "bid": block_id,
            "validators": validators,
            "level": effective_level,
            "original_level": original_level,
            "sigs": {},
            "start_time": time.time()
        }
        
        await asyncio.sleep(SIGNING_WINDOW_MS / 1000)
        
        pending = self.pending_challenges.pop(challenge, {})
        sigs = pending.get("sigs", {})
        valid_sigs = {}
        total_slashed = 0
        
        for vid, sig in sigs.items():
            if self.verify_challenge_response(vid, challenge, block_id, sig):
                valid_sigs[vid] = sig
        
        if len(valid_sigs) >= MIN_VALIDATORS_PER_BLOCK:
            block = self._add_block(block_id, self.last_hash, list(valid_sigs.keys()), effective_level, valid_sigs, challenge)
            self.distribute_block_reward(block, list(valid_sigs.keys()))
            asyncio.create_task(self.p2p.broadcast_block({
                "id": block_id,
                "ts": block.ts,
                "prev": block.prev,
                "validators": block.validators,
                "level": effective_level,
                "original_level": original_level,
                "hash": block.hash,
                "reward": block.reward,
                "challenge": challenge,
                "sigs": valid_sigs
            }))
            
            self.health_checker.record_block(block_id)
            self.metrics["blocks_produced"] += 1
            return True
        
        else:
            missing = set(validators) - set(sigs.keys())
            for vid in missing:
                total_slashed += self.slash_miner(vid, f"Missed signing for block {block_id}", block_id)
            
            if total_slashed > 0 and len(valid_sigs) > 0:
                per_signer = total_slashed // len(valid_sigs)
                for vid in valid_sigs:
                    if vid in self.miners:
                        m = self.miners[vid]
                        m.stake += per_signer
                        m.rewards += per_signer
                        m.confirmed_balance += per_signer
                        self.balances[m.wallet] = self.balances.get(m.wallet, 0) + per_signer
                        self._save_balance(m.wallet, self.balances[m.wallet])
                        self._save_miner(m)
                logger.info(f"[REDIST] {total_slashed} MCX redistributed to {len(valid_sigs)} signers")
            
            self.health_checker.record_error(f"Block {block_id} rejected")
            return False
    
    async def produce_blocks_loop(self):
        while True:
            self.update_level_groups()
            for level in range(1, 11):
                success = await self.produce_block_for_level(level)
                if success:
                    interval = self.get_block_interval(level)
                    await asyncio.sleep(interval)
            await asyncio.sleep(0.1)
    
    # ==================== REWARD DISTRIBUTION ====================
    def distribute_block_reward(self, block: Block, signers: List[str]):
        if block.reward > 0:
            return
        
        reward = self.get_current_reward_for_level(block.level)
        if reward <= 0:
            return
        
        block.reward = reward
        
        miner_total = int(reward * REWARD_MINER_SHARE)
        uptime_total = int(reward * REWARD_UPTIME_SHARE)
        node_total = int(reward * REWARD_NODE_SHARE)
        lp_total = int(reward * REWARD_LP_SHARE)
        buyer_total = int(reward * REWARD_BUYER_SHARE)
        
        miner_share = miner_total // max(len(signers), 1)
        for vid in signers:
            if vid in self.miners:
                m = self.miners[vid]
                m.rewards += miner_share
                m.stake += miner_share
                m.confirmed_balance += miner_share
                m.blocks += 1
                m.misses = 0
                m.last_block = block.id
                self.balances[m.wallet] = self.balances.get(m.wallet, 0) + miner_share
                self._save_balance(m.wallet, self.balances[m.wallet])
                self.level_mgr.register(m.wallet, m.stake)
                m.level = self.level_mgr.get_level(m.wallet)
                self._save_miner(m)
                self.conn.execute("INSERT INTO validator_history (vid, username, block_id, level, timestamp, reward) VALUES (?, ?, ?, ?, ?, ?)", 
                                 (vid, m.username, block.id, block.level, time.time(), miner_share))
                self.conn.commit()
        
        self.uptime_pool += uptime_total
        self.node_pool += node_total
        self.lp_pool += lp_total
        self.buyer_pool += buyer_total
        
        if self.validator_fee_pool > 0 and len(signers) > 0:
            fee_share = self.validator_fee_pool // len(signers)
            for vid in signers:
                if vid in self.miners:
                    m = self.miners[vid]
                    m.rewards += fee_share
                    m.stake += fee_share
                    m.confirmed_balance += fee_share
                    m.fees_collected += fee_share
                    self.balances[m.wallet] = self.balances.get(m.wallet, 0) + fee_share
                    self._save_balance(m.wallet, self.balances[m.wallet])
                    self.level_mgr.register(m.wallet, m.stake)
                    m.level = self.level_mgr.get_level(m.wallet)
                    self._save_miner(m)
            self.validator_fee_pool = 0
        
        self.update_level_supply(block.level, reward)
        self.total_minted = self.get_total_minted()
        self.metrics["rewards_distributed"] += reward
    
    def distribute_periodic_rewards(self):
        active_miners = [m for m in self.miners.values() if m.active]
        total_uptime = sum(m.uptime for m in active_miners)
        
        if total_uptime > 0 and self.uptime_pool > 0:
            for miner in active_miners:
                if miner.uptime > 0:
                    share = int(self.uptime_pool * (miner.uptime / total_uptime))
                    miner.rewards += share
                    miner.stake += share
                    miner.confirmed_balance += share
                    self.balances[miner.wallet] = self.balances.get(miner.wallet, 0) + share
                    self._save_balance(miner.wallet, self.balances[miner.wallet])
                    self.level_mgr.register(miner.wallet, miner.stake)
                    miner.level = self.level_mgr.get_level(miner.wallet)
                    self._save_miner(miner)
        
        active_nodes = [n for n in self.nodes.values() if n.active]
        if active_nodes and self.node_pool > 0:
            node_share = self.node_pool // max(len(active_nodes), 1)
            for node in active_nodes:
                node.rewards_earned += node_share
                self.balances[node.wallet] = self.balances.get(node.wallet, 0) + node_share
                self._save_balance(node.wallet, self.balances[node.wallet])
                self._save_node(node)
        
        if self.lp_pool > 0:
            total_lp_shares = 0
            for pool in self.dex.pools.values():
                for wallet, position in pool["lp_positions"].items():
                    total_lp_shares += position["shares"]
            
            if total_lp_shares > 0:
                for pool in self.dex.pools.values():
                    for wallet, position in pool["lp_positions"].items():
                        share = int(self.lp_pool * (position["shares"] / total_lp_shares))
                        if share > 0:
                            self.balances[wallet] = self.balances.get(wallet, 0) + share
                            self._save_balance(wallet, self.balances[wallet])
                            position["fees_earned"] += share
        
        self.node_pool = 0
        self.uptime_pool = 0
        self.lp_pool = 0
    
    def distribute_buyer_rewards(self):
        if self.buyer_pool == 0:
            return
        
        c = self.conn.cursor()
        c.execute("SELECT wallet, username, bought FROM buyer_stats WHERE last_reset > ? ORDER BY bought DESC", 
                 (time.time() - BUYER_REWARD_INTERVAL_DAYS * 24 * 3600,))
        buyers = c.fetchall()
        
        if not buyers:
            return
        
        total_bought = sum(b[2] for b in buyers)
        if total_bought == 0:
            return
        
        for wallet, username, bought in buyers:
            share = int(self.buyer_pool * (bought / total_bought))
            if share > 0:
                self.balances[wallet] = self.balances.get(wallet, 0) + share
                self._save_balance(wallet, self.balances[wallet])
                
                for m in self.miners.values():
                    if m.wallet == wallet:
                        m.confirmed_balance += share
                        self._save_miner(m)
                        break
        
        c.execute("UPDATE buyer_stats SET bought = 0, last_reset = ?", (time.time(),))
        self.conn.commit()
        self.buyer_pool = 0
    
    # ==================== WEBSOCKET HANDLER ====================
    async def ws_handler(self, websocket):
        if websocket.request.path != WS_PATH:
            await websocket.close(1000, "Invalid path")
            return
        
        client_ip = websocket.remote_address[0] if websocket.remote_address else "unknown"
        if not await self.rate_limiter.is_allowed(client_ip):
            await websocket.close(1008, "Rate limit exceeded")
            return
        
        logger.info(f"[WS] New connection from {client_ip}")
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    msg_type = data.get("type")
                    
                    if msg_type == "ping":
                        await websocket.send(json.dumps({"type": "pong", "timestamp": time.time()}))
                        continue
                    
                    await self._handle_ws_message(websocket, data, msg_type, client_ip)
                    
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({"type": "error", "message": "Invalid JSON"}))
                except Exception as e:
                    logger.error(f"[WS] Handler error: {e}")
                    await websocket.send(json.dumps({"type": "error", "message": str(e)}))
                    
        except websockets.exceptions.ConnectionClosed:
            logger.debug(f"[WS] Connection closed: {client_ip}")
        except Exception as e:
            logger.error(f"[WS] Connection error: {e}")
    
    async def _handle_ws_message(self, websocket, data: dict, msg_type: str, client_ip: str):
        try:
            if msg_type == "login":
                wallet = data.get("wallet")
                password = data.get("password")
                
                if not wallet or not password:
                    await websocket.send(json.dumps({"type": "login_result", "success": False, "error": "Wallet and password required"}))
                    return
                
                c = self.conn.cursor()
                c.execute("SELECT * FROM genesis_wallet WHERE wallet = ? AND password_hash = ?", 
                         (wallet, sha256_hash(password)))
                if c.fetchone():
                    private_key = self.get_private_key(wallet, password)
                    await websocket.send(json.dumps({
                        "type": "login_result",
                        "success": True,
                        "wallet": wallet,
                        "balance": self.get_confirmed_balance(wallet),
                        "staked": 0,
                        "level": 1,
                        "is_genesis": True,
                        "private_key": private_key,
                        "message": "Welcome Genesis Node Operator!"
                    }))
                    return
                
                if self.login(wallet, password):
                    balance = self.get_confirmed_balance(wallet)
                    staked = 0
                    private_key = self.get_private_key(wallet, password)
                    for m in self.miners.values():
                        if m.wallet == wallet:
                            staked = m.stake
                            break
                    
                    await websocket.send(json.dumps({
                        "type": "login_result",
                        "success": True,
                        "wallet": wallet,
                        "balance": balance,
                        "staked": staked,
                        "level": self.level_mgr.get_level(wallet),
                        "is_genesis": False,
                        "private_key": private_key
                    }))
                else:
                    await websocket.send(json.dumps({"type": "login_result", "success": False, "error": "Invalid credentials"}))
            
            # ============================================================
            # ✅ COMPLETE FIX: REGISTER HANDLER
            # ============================================================
            elif msg_type == "register":
                logger.info(f"[WS] 📝 REGISTRATION RECEIVED from {data.get('username', 'unknown')}")
                logger.info(f"[WS] Wallet: {data.get('wallet', 'unknown')}")
                logger.info(f"[WS] Validator ID: {data.get('validator_id', 'unknown')}")
                logger.info(f"[WS] Stake: {data.get('stake', 0)}")
                logger.info(f"[WS] Miner Type: {data.get('miner_type', 'web')}")
                
                # Check if this wallet already exists
                existing_miner = None
                for m in self.miners.values():
                    if m.wallet == data["wallet"]:
                        existing_miner = m
                        break
                
                if existing_miner:
                    logger.info(f"[WS] ⚠️ Miner already exists: {existing_miner.username}, updating...")
                    existing_miner.last_ping = time.time()
                    existing_miner.active = True
                    self._save_miner(existing_miner)
                    await websocket.send(json.dumps({
                        "type": "registered",
                        "level": existing_miner.level,
                        "max_level": self.level_mgr.max_unlocked,
                        "remaining_supply": self.get_remaining_supply_for_level(1),
                        "current_reward": self.get_current_reward_for_level(1),
                        "confirmed_balance": existing_miner.confirmed_balance,
                        "message": "Miner already registered, updated"
                    }))
                    return
                
                ok = self.register_miner(
                    data["validator_id"],
                    data["public_key"],
                    data["username"],
                    data["wallet"],
                    data["stake"],
                    data["signature"],
                    data["timestamp"],
                    data.get("miner_type", "web")
                )
                
                logger.info(f"[WS] Registration result: {ok}")
                
                if ok:
                    miner_added = False
                    for m in self.miners.values():
                        if m.wallet == data["wallet"]:
                            miner_added = True
                            logger.info(f"[WS] ✅ Miner added successfully: {m.username}")
                            break
                    
                    if not miner_added:
                        logger.error(f"[WS] ❌ Miner was not added to database!")
                        await websocket.send(json.dumps({
                            "type": "registration_failed",
                            "error": "Miner not added to database"
                        }))
                        return
                    
                    await websocket.send(json.dumps({
                        "type": "registered",
                        "level": self.level_mgr.get_level(data["wallet"]),
                        "max_level": self.level_mgr.max_unlocked,
                        "remaining_supply": self.get_remaining_supply_for_level(1),
                        "current_reward": self.get_current_reward_for_level(1),
                        "dex_pools": self.dex.get_pools(),
                        "bridge_status": self.bridge.get_bridge_status(),
                        "confirmed_balance": self.get_confirmed_balance(data["wallet"]),
                        "message": "Registration successful!"
                    }))
                else:
                    logger.warning(f"[WS] ❌ Registration failed for {data.get('username', 'unknown')}")
                    await websocket.send(json.dumps({
                        "type": "registration_failed",
                        "error": "Registration failed - check signature or banned"
                    }))
            
            elif msg_type == "block_signature":
                ch = data["challenge"]
                if ch in self.pending_challenges:
                    self.pending_challenges[ch]["sigs"][data["validator_id"]] = data["signature"]
            
            elif msg_type == "uptime_ping":
                self.update_miner_uptime(data["validator_id"], data.get("uptime_seconds", 0), data.get("today_uptime", 0))
            
            elif msg_type == "get_genesis_info":
                c = self.conn.cursor()
                c.execute("SELECT * FROM genesis_wallet ORDER BY created_at DESC LIMIT 1")
                row = c.fetchone()
                if row:
                    await websocket.send(json.dumps({
                        "type": "genesis_info",
                        "wallet": row[1],
                        "username": row[2],
                        "created_at": row[3]
                    }))
                else:
                    await websocket.send(json.dumps({"type": "genesis_info", "error": "No genesis wallet found"}))
            
            elif msg_type == "stake":
                wallet = data.get("wallet")
                amount = data.get("amount")
                signature = data.get("signature", "")
                timestamp = data.get("timestamp", int(time.time()))
                
                result = self.process_stake(wallet, amount, signature, timestamp)
                await websocket.send(json.dumps({"type": "staking_confirmed", **result}))
            
            elif msg_type == "unstake":
                wallet = data.get("wallet")
                amount = data.get("amount")
                signature = data.get("signature", "")
                timestamp = data.get("timestamp", int(time.time()))
                
                result = self.process_unstake(wallet, amount, signature, timestamp)
                await websocket.send(json.dumps({"type": "staking_confirmed", **result}))
            
            elif msg_type == "send":
                from_wallet = data.get("from_wallet")
                to_wallet = data.get("to_wallet")
                amount = data.get("amount")
                signature = data.get("signature", "")
                timestamp = data.get("timestamp", int(time.time()))
                
                result = self.send_mcx(from_wallet, to_wallet, amount, signature, timestamp)
                await websocket.send(json.dumps({"type": "send_result", **result}))
            
            elif msg_type == "get_balance":
                wallet = data.get("wallet")
                balance = self.get_balance(wallet)
                confirmed = self.get_confirmed_balance(wallet)
                staked = 0
                for m in self.miners.values():
                    if m.wallet == wallet:
                        staked = m.stake
                        break
                await websocket.send(json.dumps({"type": "balance", "balance": balance, "confirmed": confirmed, "staked": staked}))
            
            elif msg_type == "get_miners":
                miners = self.get_miners_list()
                await websocket.send(json.dumps({"type": "miners", "miners": miners}))
            
            elif msg_type == "get_nodes":
                nodes = self.get_nodes_list()
                await websocket.send(json.dumps({"type": "nodes", "nodes": nodes}))
            
            elif msg_type == "get_top_stakers":
                stakers = self.get_top_stakers(10)
                await websocket.send(json.dumps({"type": "top_stakers", "stakers": stakers}))
            
            elif msg_type == "get_top_buyers":
                buyers = self.get_buyer_stats(10)
                await websocket.send(json.dumps({"type": "top_buyers", "buyers": buyers}))
            
            elif msg_type == "get_status":
                total_supply = 0
                for level in range(1, 11):
                    total_supply += self.get_remaining_supply_for_level(level)
                cleaned = self.cleanup_inactive_miners()
                
                await websocket.send(json.dumps({
                    "type": "status",
                    "data": {
                        "block_id": self.height,
                        "total_miners": len(self.miners),
                        "active_miners": sum(1 for m in self.miners.values() if m.active),
                        "total_nodes": len(self.nodes),
                        "active_nodes": sum(1 for n in self.nodes.values() if n.active),
                        "max_level": self.level_mgr.max_unlocked,
                        "current_reward": self.get_current_reward_for_level(1),
                        "total_minted": self.total_minted,
                        "remaining_supply": total_supply,
                        "node_pool": self.node_pool,
                        "uptime_pool": self.uptime_pool,
                        "lp_pool": self.lp_pool,
                        "buyer_pool": self.buyer_pool,
                        "validator_fee_pool": self.validator_fee_pool,
                        "levels_with_miners": sorted(self.levels_with_miners),
                        "pools": self.dex.get_pools(),
                        "bridge_status": self.bridge.get_bridge_status(),
                        "duco_pool_balance": self.bridge.get_duco_pool_balance(),
                        "cleanup_removed": cleaned,
                        "mempool_size": self.mempool.size(),
                        "peers": len(self.p2p.peers),
                        "uptime": int(time.time() - self.start_time),
                        "genesis_wallet": self.conn.execute("SELECT wallet, username FROM genesis_wallet ORDER BY created_at DESC LIMIT 1").fetchone()[0] if self.conn.execute("SELECT COUNT(*) FROM genesis_wallet").fetchone()[0] > 0 else None
                    }
                }))
            
            # DEX MESSAGES
            elif msg_type == "get_pools":
                result = self.dex.get_pools()
                await websocket.send(json.dumps({"type": "pools", "data": result}))
            
            elif msg_type == "get_tokens":
                result = self.dex.get_tokens()
                await websocket.send(json.dumps({"type": "tokens", "data": result}))
            
            elif msg_type == "get_user_lp":
                wallet = data.get("wallet")
                result = self.dex.get_user_lp(wallet)
                await websocket.send(json.dumps({"type": "user_lp", "data": result}))
            
            elif msg_type == "get_user_tokens":
                wallet = data.get("wallet")
                result = self.dex.get_user_token_balances(wallet)
                await websocket.send(json.dumps({"type": "user_tokens", "data": result}))
            
            elif msg_type == "get_price_history":
                pool_id = data.get("pool_id")
                hours = data.get("hours", 24)
                result = self.dex.get_price_history(pool_id, hours)
                await websocket.send(json.dumps({"type": "price_history", "data": result}))
            
            elif msg_type == "get_quote":
                pool_id = data.get("pool_id")
                token_in = data.get("token_in")
                token_out = data.get("token_out")
                amount_in = data.get("amount_in", 0)
                result = self.dex.get_quote(pool_id, token_in, token_out, amount_in)
                await websocket.send(json.dumps({"type": "quote", "data": result}))
            
            elif msg_type == "swap":
                pool_id = data.get("pool_id")
                token_in = data.get("token_in")
                token_out = data.get("token_out")
                amount_in = data.get("amount_in", 0)
                user = data.get("wallet")
                slippage = data.get("slippage", 0.005)
                result = await self.dex.swap(pool_id, token_in, token_out, amount_in, user, slippage)
                await websocket.send(json.dumps({"type": "swap_result", "data": result}))
            
            elif msg_type == "add_liquidity":
                pool_id = data.get("pool_id")
                amount_a = data.get("amount_a", 0)
                amount_b = data.get("amount_b", 0)
                user = data.get("wallet")
                result = await self.dex.add_liquidity(pool_id, amount_a, amount_b, user)
                await websocket.send(json.dumps({"type": "liquidity_added", "data": result}))
            
            elif msg_type == "remove_liquidity":
                pool_id = data.get("pool_id")
                lp_shares = data.get("lp_shares", 0)
                user = data.get("wallet")
                result = await self.dex.remove_liquidity(pool_id, lp_shares, user)
                await websocket.send(json.dumps({"type": "liquidity_removed", "data": result}))
            
            # BRIDGE MESSAGES
            elif msg_type == "create_multisig_proposal":
                token_symbol = data.get("token_symbol", "").upper()
                action = data.get("action", "")
                amount = data.get("amount", 0)
                recipient = data.get("recipient", "")
                node_id = data.get("node_id", "")
                memo = data.get("memo", "")
                
                result = await self.bridge.create_multisig_proposal(token_symbol, action, amount, recipient, node_id, memo)
                await websocket.send(json.dumps({"type": "multisig_proposal_result", "data": result}))
            
            elif msg_type == "approve_multisig_proposal":
                proposal_id = data.get("proposal_id", "")
                node_id = data.get("node_id", "")
                
                result = await self.bridge.approve_multisig_proposal(proposal_id, node_id)
                await websocket.send(json.dumps({"type": "multisig_approval_result", "data": result}))
            
            elif msg_type == "get_multisig_proposals":
                status = data.get("status", "pending")
                proposals = self.bridge.get_multisig_proposals(status)
                await websocket.send(json.dumps({"type": "multisig_proposals", "data": proposals}))
            
            elif msg_type == "get_bridge_status":
                status = self.bridge.get_bridge_status()
                await websocket.send(json.dumps({"type": "bridge_status", "data": status}))
            
            elif msg_type == "sell_mcx":
                wallet = data.get("wallet")
                amount = data.get("amount")
                
                miner = None
                for m in self.miners.values():
                    if m.wallet == wallet:
                        miner = m
                        break
                
                if not miner:
                    await websocket.send(json.dumps({"type": "sell_result", "success": False, "error": "Wallet not found"}))
                    return
                
                if miner.confirmed_balance < amount:
                    await websocket.send(json.dumps({"type": "sell_result", "success": False, "error": f"Insufficient confirmed balance. You have {miner.confirmed_balance} MCX"}))
                    return
                
                mcx_price = self.dex.get_token_price("MCX", "DUCO")
                if mcx_price <= 0:
                    mcx_price = 0.1
                
                duco_amount = amount * mcx_price
                
                if self.bridge.duco_pool_balance < duco_amount:
                    await websocket.send(json.dumps({"type": "sell_result", "success": False, "error": f"Insufficient DUCO pool liquidity. Available: {self.bridge.duco_pool_balance} DUCO"}))
                    return                
                miner.confirmed_balance -= amount
                self.balances[wallet] = miner.confirmed_balance
                self._save_miner(miner)
                self._save_balance(wallet, miner.confirmed_balance)
                
                self.bridge.duco_pool_balance -= duco_amount
                
                await websocket.send(json.dumps({
                    "type": "sell_result",
                    "success": True,
                    "mcx_amount": amount,
                    "duco_amount": duco_amount,
                    "price": mcx_price,
                    "confirmed_balance": miner.confirmed_balance
                }))
            
            elif msg_type == "get_duco_pool":
                await websocket.send(json.dumps({
                    "type": "duco_pool",
                    "balance": self.bridge.duco_pool_balance,
                    "address": DUCO_PAYMENT_ADDRESS,
                    "mcx_price": self.dex.get_token_price("MCX", "DUCO")
                }))
            
            elif msg_type == "verify_duco":
                wallet = data.get("wallet")
                duco_username = data.get("duco_username")
                txid = data.get("txid")
                tx_time = data.get("tx_time")
                amount = data.get("amount")
                
                c = self.conn.cursor()
                c.execute("INSERT OR IGNORE INTO duco_payments (txid, wallet, username, amount, memo, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                         (txid, wallet, duco_username, amount, "swap", tx_time))
                self.conn.commit()
                
                mcx_price = self.dex.get_token_price("MCX", "DUCO")
                if mcx_price <= 0:
                    mcx_price = 0.1
                
                mcx_amount = int(amount * mcx_price)
                
                self.balances[wallet] = self.balances.get(wallet, 0) + mcx_amount
                self._save_balance(wallet, self.balances[wallet])
                
                for m in self.miners.values():
                    if m.wallet == wallet:
                        m.confirmed_balance += mcx_amount
                        self._save_miner(m)
                        break
                
                await websocket.send(json.dumps({
                    "type": "duco_result",
                    "data": {
                        "success": True,
                        "message": f"Payment verified! +{mcx_amount} MCX credited at price {mcx_price} DUCO/MCX",
                        "txid": txid,
                        "duco_amount": amount,
                        "mcx_amount": mcx_amount,
                        "price": mcx_price,
                        "duco_username": duco_username
                    }
                }))
            
            elif msg_type == "buy_mcx_crypto":
                wallet = data.get("wallet")
                username = data.get("username")
                usd_amount = data.get("usd_amount")
                method = data.get("method")
                sender_address = data.get("sender_address")
                txid = data.get("txid", "")
                
                usdt_price = self.dex.get_token_price("MCX", "USDT")
                if usdt_price <= 0:
                    usdt_price = 0.01
                
                mcx_amount = int(usd_amount / usdt_price)
                
                self.balances[wallet] = self.balances.get(wallet, 0) + mcx_amount
                self._save_balance(wallet, self.balances[wallet])
                
                for m in self.miners.values():
                    if m.wallet == wallet:
                        m.confirmed_balance += mcx_amount
                        self._save_miner(m)
                        break
                
                await websocket.send(json.dumps({
                    "type": "payment_check",
                    "data": {
                        "success": True,
                        "mcx_amount": mcx_amount,
                        "method": method,
                        "price": usdt_price,
                        "message": f"Payment verified! +{mcx_amount} MCX credited at price {usdt_price} USDT/MCX"
                    }
                }))
            
            elif msg_type == "control_miner":
                vid = data.get("miner_id")
                action = data.get("action")
                miner = self.miners.get(vid)
                if not miner:
                    await websocket.send(json.dumps({"type": "control_result", "success": False, "message": "Miner not found"}))
                    return
                
                if action == "stop":
                    miner.active = False
                    self.conn.execute("UPDATE miners SET active=0 WHERE vid=?", (vid,))
                    self.conn.commit()
                elif action == "start":
                    miner.active = True
                    miner.banned_until = 0
                    self.conn.execute("UPDATE miners SET active=1, banned_until=0 WHERE vid=?", (vid,))
                    self.conn.commit()
                elif action == "restart":
                    miner.active = False
                    self.conn.execute("UPDATE miners SET active=0 WHERE vid=?", (vid,))
                    self.conn.commit()
                    await asyncio.sleep(1)
                    miner.active = True
                    miner.banned_until = 0
                    self.conn.execute("UPDATE miners SET active=1, banned_until=0 WHERE vid=?", (vid,))
                    self.conn.commit()
                
                await websocket.send(json.dumps({"type": "control_result", "miner_id": vid, "action": action, "success": True}))
            
            # ✅ ADD THIS: Handle bridge registration
            elif msg_type == "register_bridge":
                logger.info(f"[WS] Bridge registered from {client_ip}")
                await websocket.send(json.dumps({
                    "type": "registered",
                    "message": "Bridge connected successfully",
                    "version": VERSION,
                    "timestamp": time.time()
                }))
                return
            
            else:
                await websocket.send(json.dumps({"type": "error", "message": f"Unknown message type: {msg_type}"}))
                
        except Exception as e:
            logger.error(f"[WS] Message handler error: {e}")
            await websocket.send(json.dumps({"type": "error", "message": str(e)}))

# ==================== MAIN SERVER ====================
class MicroCoreServer:
    def __init__(self, network: MicroCoreNetwork):
        self.network = network
        self._shutdown = False
        self._tasks = []
    
    async def block_production_loop(self):
        while not self._shutdown:
            try:
                await self.network.produce_blocks_loop()
            except Exception as e:
                logger.error(f"[PRODUCER] Error: {e}")
                await asyncio.sleep(5)
    
    async def peer_discovery_loop(self):
        while not self._shutdown:
            try:
                await self.network.p2p.discover()
                await asyncio.sleep(DISCOVERY_INTERVAL)
            except Exception as e:
                logger.error(f"[PEER] Discovery error: {e}")
                await asyncio.sleep(5)
    
    async def peer_sync_loop(self):
        while not self._shutdown:
            try:
                await self.network.p2p.sync_with_peers()
                await asyncio.sleep(SYNC_INTERVAL)
            except Exception as e:
                logger.error(f"[PEER] Sync error: {e}")
                await asyncio.sleep(5)
    
    async def periodic_distribution_loop(self):
        while not self._shutdown:
            try:
                await asyncio.sleep(DISTRIBUTION_INTERVAL_SEC)
                self.network.distribute_periodic_rewards()
            except Exception as e:
                logger.error(f"[DISTRO] Error: {e}")
    
    async def buyer_rewards_loop(self):
        while not self._shutdown:
            try:
                await asyncio.sleep(3600)
                if time.time() - self.network.last_buyer_distribution > BUYER_REWARD_INTERVAL_DAYS * 24 * 3600:
                    self.network.distribute_buyer_rewards()
                    self.network.last_buyer_distribution = time.time()
            except Exception as e:
                logger.error(f"[BUYER] Error: {e}")
    
    async def embedded_miner_loop(self):
        while not self._shutdown:
            try:
                current_time = time.time()
                if current_time - self.network._last_self_ping > 30:
                    self.network._last_self_ping = current_time
                    if self.network.wallet in self.network.miners:
                        miner = self.network.miners[self.network.wallet]
                        miner.last_ping = current_time
                        miner.uptime += 30
                        miner.today_uptime += 30
                        miner.active = True
                        self.network._save_miner(miner)
                
                for challenge, pending in self.network.pending_challenges.items():
                    vid = self.network.wallet
                    if vid in pending["validators"] and vid not in pending["sigs"]:
                        message = f"{challenge}{vid}{pending['bid']}"
                        signature = sign_message(self.network.priv, message)
                        pending["sigs"][vid] = signature
                        logger.info(f"[EMBEDDED MINER] Signed block {pending['bid']}")
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.error(f"[EMBEDDED] Error: {e}")
                await asyncio.sleep(1)
    
    async def cleanup_loop(self):
        while not self._shutdown:
            try:
                await asyncio.sleep(60)
                cleaned = self.network.cleanup_inactive_miners()
                if cleaned > 0:
                    logger.info(f"[CLEANUP] Removed {cleaned} inactive miners")
                await self.network.rate_limiter.cleanup()
            except Exception as e:
                logger.error(f"[CLEANUP] Error: {e}")
    
    async def status_reporter_loop(self):
        while not self._shutdown:
            try:
                await asyncio.sleep(60)
                total_minted = self.network.total_minted
                total_cap = sum(LEVEL_CAPS.values())
                percent = (total_minted / total_cap) * 100 if total_cap > 0 else 0
                remaining = total_cap - total_minted
                
                logger.info(f"\n{'='*60}")
                logger.info(f"MICROCORE NETWORK STATUS")
                logger.info(f"{'='*60}")
                logger.info(f"Block Height: {self.network.height}")
                logger.info(f"Total Minted: {total_minted:,} / {total_cap:,} ({percent:.4f}%)")
                logger.info(f"Remaining Supply: {remaining:,} MCX")
                logger.info(f"Active Miners: {sum(1 for m in self.network.miners.values() if m.active)}")
                logger.info(f"Total Miners: {len(self.network.miners)}")
                logger.info(f"Active Nodes: {sum(1 for n in self.network.nodes.values() if n.active)}")
                logger.info(f"Total Nodes: {len(self.network.nodes)}")
                logger.info(f"P2P Peers: {len(self.network.p2p.peers)}")
                logger.info(f"Max Unlocked Level: {self.network.level_mgr.max_unlocked}")
                logger.info(f"Levels with Miners: {sorted(self.network.levels_with_miners)}")
                logger.info(f"DUCO Pool Balance: {self.network.bridge.duco_pool_balance} DUCO")
                logger.info(f"Mempool Size: {self.network.mempool.size()}")
                logger.info(f"Health: {self.network.health_checker.get_status()}")
                logger.info(f"Multisig Bridge Signers: {len(self.network.bridge.get_multisig_signers())}/{MULTISIG_TOTAL_SIGNERS}")
                
                for level in range(1, 11):
                    remaining_supply = self.network.get_remaining_supply_for_level(level)
                    cap = LEVEL_CAPS[level]
                    level_percent = ((cap - remaining_supply) / cap) * 100 if cap > 0 else 0
                    miner_count = len(self.network.level_groups.get(level, []))
                    diff_wallets = len(self.network.level_mgr.level_wallets.get(level, set()))
                    reward = self.network.get_current_reward_for_level(level)
                    logger.info(f"Level {level}: {miner_count} miners | {diff_wallets} different wallets | {remaining_supply:,} / {cap:,} MCX remaining ({level_percent:.1f}%) | Reward: {reward} MCX")
                
                logger.info(f"\n[DEX PRICES]")
                for pool_id, pool in self.network.dex.pools.items():
                    if pool["reserve_a"] > 0 and pool["reserve_b"] > 0:
                        price = pool["reserve_a"] / pool["reserve_b"]
                        logger.info(f"  {pool_id}: {price:.6f} (Liquidity: {pool['reserve_a']} + {pool['reserve_b']})")
                    else:
                        logger.info(f"  {pool_id}: No liquidity")
                
                logger.info(f"{'='*60}\n")
                
            except Exception as e:
                logger.error(f"[STATUS] Error: {e}")
    
    async def cleanup(self):
        self._shutdown = True
        for task in self._tasks:
            task.cancel()
        if self.network.conn:
            self.network.conn.close()
        logger.info("[SHUTDOWN] Cleanup complete")
    
    async def run(self):
        logger.info(f"\n{'='*60}")
        logger.info(f"MICROCORE (MCX) NODE v{VERSION}")
        logger.info(f"{'='*60}")
        logger.info(f"Wallet: {self.network.wallet}")
        logger.info(f"Display Name: {self.network.display_name}")
        logger.info(f"Node ID: {self.network.node_id[:16]}...")
        logger.info(f"{'='*60}")
        logger.info(f"WebSocket: ws://127.0.0.1:{NODE_PORT}{WS_PATH}")
        logger.info(f"P2P: 127.0.0.1:{P2P_PORT}")
        logger.info(f"Bootnodes: {BOOTSTRAP_NODES}")
        logger.info(f"GOSSIP DISCOVERY: ON")
        logger.info(f"EMBEDDED MINER: ACTIVE")
        logger.info(f"RATE LIMITING: ENABLED (60 req/min per IP)")
        logger.info(f"{'='*60}")
        logger.info(f"✅ PRICE DISCOVERY: ZERO STARTING PRICE")
        logger.info(f"   All DEX pools start with 0 liquidity")
        logger.info(f"   Price is determined solely by users adding liquidity")
        logger.info(f"   First liquidity provider sets the initial price")
        logger.info(f"   No hardcoded prices anywhere in the system")
        logger.info(f"{'='*60}")
        logger.info(f"SECURITY: ECDSA secp256k1 signatures + AVR DJB2 support")
        logger.info(f"All transfers require cryptographic authorization")
        logger.info(f"All blocks validated with signature verification")
        logger.info(f"All miner registrations require valid signatures")
        logger.info(f"{'='*60}")
        logger.info(f"MULTISIG BRIDGE: {MULTISIG_THRESHOLD} of {MULTISIG_TOTAL_SIGNERS} signatures required")
        logger.info(f"BRIDGE ADDRESSES (MULTISIG):")
        for symbol, address in BRIDGE_ADDRESSES.items():
            logger.info(f"  {symbol}: {address}")
        logger.info(f"{'='*60}")
        logger.info(f"REWARD DISTRIBUTION:")
        logger.info(f"  75% -> Miners (validators)")
        logger.info(f"  20.999% -> Uptime Rewards (miners)")
        logger.info(f"   3% -> Nodes")
        logger.info(f"   1% -> Liquidity Providers")
        logger.info(f"   0.001% -> Buyer Rewards (weekly)")
        logger.info(f"{'='*60}")
        logger.info(f"LEVEL SYSTEM: 10 levels, 1,000 MCX per level")
        logger.info(f"Each level requires {MIN_WALLETS_FOR_NEXT_LEVEL} DIFFERENT wallets")
        logger.info(f"BLOCK TIMES: L1:40s, L2:35s, L3:30s, L4:25s, L5:20s, L6:15s, L7:10s, L8:9s, L9:8s, L10:7s")
        logger.info(f"BLOCK REWARD: 18 MCX (halving per level)")  
        logger.info(f"TOTAL CAP: ~3,281,040,000 MCX")
        logger.info(f"{'='*60}")
        logger.info(f"Node is running! Press Ctrl+C to stop.\n")
        
        self._tasks = [
            asyncio.create_task(self.network.p2p.start()),
            asyncio.create_task(self.network.p2p.heartbeat()),
            asyncio.create_task(self.peer_discovery_loop()),
            asyncio.create_task(self.peer_sync_loop()),
            asyncio.create_task(self.periodic_distribution_loop()),
            asyncio.create_task(self.buyer_rewards_loop()),
            asyncio.create_task(self.block_production_loop()),
            asyncio.create_task(self.embedded_miner_loop()),
            asyncio.create_task(self.cleanup_loop()),
            asyncio.create_task(self.status_reporter_loop()),
            asyncio.create_task(self.network.rate_limiter.start_cleanup()),
        ]
        
        async with serve(self.network.ws_handler, "127.0.0.1", NODE_PORT):
            logger.info(f"[WS] WebSocket server started on ws://127.0.0.1:{NODE_PORT}{WS_PATH}")
            await asyncio.Future()

# ==================== MAIN ====================
async def main():
    parser = argparse.ArgumentParser(description=f'{NAME} Complete Node v{VERSION}')
    parser.add_argument('--genesis', action='store_true', help='Run as genesis node (first node)')
    parser.add_argument('--peer', type=str, help='Connect to peer node (IP:PORT)')
    parser.add_argument('--wallet', type=str, default="", help='Your wallet address')
    parser.add_argument('--display-name', type=str, default="", help='Your display name')
    parser.add_argument('--password', type=str, default="", help='Your password for login')
    parser.add_argument('--privkey', type=str, default="", help='Your private key (optional)')
    parser.add_argument('--no-miner', action='store_true', help='Disable embedded miner')
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"{NAME} ({SYMBOL}) COMPLETE NODE v{VERSION}")
    print(f"{'='*60}")
    print(f"✅ ZERO STARTING PRICE - All DEX pools start with 0 liquidity")
    print(f"✅ Price will be discovered when users add liquidity to pools")
    print(f"✅ No hardcoded prices anywhere in the system")
    print(f"{'='*60}")
    
    if not args.wallet:
        my_priv, my_pub = generate_keypair()
        args.wallet = "MCR_" + hashlib.sha256(my_pub.encode()).hexdigest()[:32].upper()
        args.privkey = my_priv
        print(f" Generated Wallet: {args.wallet}")
        print(f" Private Key: {my_priv[:16]}... (encrypted)")
    
    if not validate_wallet_address(args.wallet):
        print(f" Invalid wallet address: {args.wallet}")
        print("   Wallet addresses must start with 'MCR_' and be 36 characters long.")
        sys.exit(1)
    
    print(f"Wallet: {args.wallet}")
    print(f"Display Name: {args.display_name or args.wallet[:12]}")
    print(f"Genesis Mode: {args.genesis}")
    print(f"Password: {'SET' if args.password else 'NOT SET'}")
    print(f"Embedded Miner: {'DISABLED' if args.no_miner else 'ACTIVE'}")
    print(f"DEX: ENABLED (MCX/USDT, MCX/BTC, MCX/POL, MCX/TRX, MCX/DUCO)")
    print(f"MULTISIG BRIDGE: {MULTISIG_THRESHOLD} of {MULTISIG_TOTAL_SIGNERS} signatures required")
    print(f"{'='*60}\n")
    
    wallet_file = f"microcore_wallet_{args.wallet[:16]}.json"
    
    if args.wallet and args.privkey:
        my_wallet = args.wallet
        my_priv = args.privkey
        priv_obj = ec.derive_private_key(int(my_priv, 16), ec.SECP256K1())
        pub = priv_obj.public_key()
        my_pub = pub.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    elif args.wallet and os.path.exists(wallet_file):
        with open(wallet_file, 'r') as f:
            wallet_data = json.load(f)
            my_wallet = wallet_data["address"]
            my_priv = decrypt_private_key(
                wallet_data["private_key_encrypted"],
                args.wallet + "_microcore_v15"
            )
            my_pub = wallet_data["public_key_pem"]
    else:
        my_priv, my_pub = generate_keypair()
        my_wallet = args.wallet
        
        encrypted_priv = encrypt_private_key(my_priv, args.wallet + "_microcore_v15")
        wallet_data = {
            "address": my_wallet,
            "public_key_pem": my_pub,
            "private_key_encrypted": encrypted_priv,
            "created_at": time.time(),
            "version": VERSION
        }
        with open(wallet_file, 'w') as f:
            json.dump(wallet_data, f, indent=2)
        logger.info(f"[WALLET] Encrypted wallet saved to: {wallet_file}")
    
    display_name = args.display_name if args.display_name else args.wallet[:12]
    password = args.password if args.password else "microcore_default"
    
    network = MicroCoreNetwork(
        is_genesis=args.genesis,
        display_name=display_name,
        wallet=args.wallet,
        priv=my_priv,
        pub=my_pub,
        password=password
    )
    
    server = MicroCoreServer(network)
    
    if args.peer:
        logger.info(f"[P2P] Connecting to peer: {args.peer}")
        await network.p2p._connect(args.peer)
    
    try:
        await server.run()
    except asyncio.CancelledError:
        logger.info("[SHUTDOWN] Server stopped")
    finally:
        await server.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Node stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        traceback.print_exc()
        sys.exit(1)
