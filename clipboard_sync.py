#!/usr/bin/env python3
# /// script
# dependencies = ["zeroconf"]
# ///
"""Peer-to-peer LAN clipboard sync using Bonjour discovery and raw TCP."""

import hashlib
import socket
import struct
import subprocess
import threading
import time
import uuid

from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf

SERVICE_TYPE = "_clipsync._tcp.local."
POLL_INTERVAL = 0.5
INSTANCE_ID = uuid.uuid4().hex[:8]


def get_clipboard():
    try:
        return subprocess.run(
            ["pbpaste"], capture_output=True, text=True, timeout=2
        ).stdout
    except Exception:
        return ""


def set_clipboard(text):
    try:
        subprocess.run(
            ["pbcopy"], input=text, text=True, timeout=2, check=True
        )
    except Exception:
        pass


def content_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def send_message(sock, data: bytes):
    sock.sendall(struct.pack("!I", len(data)) + data)


def recv_message(sock) -> bytes | None:
    header = b""
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        if not chunk:
            return None
        header += chunk
    length = struct.unpack("!I", header)[0]
    if length > 10 * 1024 * 1024:  # 10MB sanity limit
        return None
    data = b""
    while len(data) < length:
        chunk = sock.recv(min(length - len(data), 65536))
        if not chunk:
            return None
        data += chunk
    return data


class ClipSync:
    def __init__(self):
        self.peers: dict[str, tuple[str, int]] = {}  # name -> (host, port)
        self.lock = threading.Lock()
        self.last_hash = content_hash(get_clipboard())
        self.running = True

    def start(self):
        # Start TCP listener on random port
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(("", 0))
        self.server_sock.listen(5)
        self.port = self.server_sock.getsockname()[1]
        self.server_sock.settimeout(1.0)

        # Register Bonjour service
        hostname = socket.gethostname()
        self.service_name = f"ClipSync-{INSTANCE_ID}.{hostname}"
        self.zeroconf = Zeroconf()
        self.info = ServiceInfo(
            SERVICE_TYPE,
            f"{self.service_name}.{SERVICE_TYPE}",
            port=self.port,
            server=f"{hostname}.local.",
        )
        self.zeroconf.register_service(self.info)
        print(f"[*] Registered as {self.service_name} on port {self.port}")

        # Browse for peers
        self.browser = ServiceBrowser(self.zeroconf, SERVICE_TYPE, self)

        # Start threads
        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(target=self._poll_clipboard, daemon=True).start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[*] Shutting down...")
        finally:
            self.running = False
            self.zeroconf.unregister_service(self.info)
            self.zeroconf.close()
            self.server_sock.close()

    # --- Zeroconf ServiceListener callbacks ---

    def add_service(self, zc: Zeroconf, type_: str, name: str):
        info = zc.get_service_info(type_, name)
        if not info or not info.addresses:
            return
        if name == f"{self.service_name}.{SERVICE_TYPE}":
            return  # skip self
        host = socket.inet_ntoa(info.addresses[0])
        port = info.port
        with self.lock:
            self.peers[name] = (host, port)
        print(f"[+] Discovered peer: {name} at {host}:{port}")

    def remove_service(self, zc: Zeroconf, type_: str, name: str):
        with self.lock:
            if name in self.peers:
                del self.peers[name]
        print(f"[-] Peer left: {name}")

    def update_service(self, zc: Zeroconf, type_: str, name: str):
        self.add_service(zc, type_, name)

    # --- TCP listener ---

    def _accept_loop(self):
        while self.running:
            try:
                conn, addr = self.server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(
                target=self._handle_conn, args=(conn, addr), daemon=True
            ).start()

    def _handle_conn(self, conn, addr):
        try:
            data = recv_message(conn)
            if data is not None:
                text = data.decode("utf-8")
                h = content_hash(text)
                with self.lock:
                    if h != self.last_hash:
                        self.last_hash = h
                        set_clipboard(text)
                        print(f"[<] Received clipboard from {addr[0]} ({len(text)} chars)")
        except Exception as e:
            print(f"[!] Error receiving from {addr}: {e}")
        finally:
            conn.close()

    # --- Clipboard polling ---

    def _poll_clipboard(self):
        while self.running:
            time.sleep(POLL_INTERVAL)
            text = get_clipboard()
            h = content_hash(text)
            with self.lock:
                if h == self.last_hash:
                    continue
                self.last_hash = h
                peers = dict(self.peers)
            if not peers:
                continue
            data = text.encode("utf-8")
            print(f"[>] Sending clipboard to {len(peers)} peer(s) ({len(text)} chars)")
            for name, (host, port) in peers.items():
                self._send_to_peer(name, host, port, data)

    def _send_to_peer(self, name, host, port, data):
        try:
            with socket.create_connection((host, port), timeout=3) as sock:
                send_message(sock, data)
        except Exception as e:
            print(f"[!] Failed to send to {host}:{port}: {e}")


if __name__ == "__main__":
    ClipSync().start()
