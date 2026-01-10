import socket
import json
from typing import Any, Optional


class MagicQueueClient:
    """TCP client for MagicQueue server."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6789,
        unix_socket: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.unix_socket = unix_socket
        self._socket: Optional[socket.socket] = None
        self._buffer = b""

    def connect(self) -> None:
        """Connect to the MagicQueue server."""
        if self._socket:
            return

        if self.unix_socket:
            self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._socket.connect(self.unix_socket)
        else:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._socket.connect((self.host, self.port))

    def close(self) -> None:
        """Close the connection."""
        if self._socket:
            self._socket.close()
            self._socket = None

    def is_connected(self) -> bool:
        """Check if connected."""
        return self._socket is not None

    def send(self, command: dict) -> dict:
        """Send a command and receive response."""
        if not self._socket:
            raise ConnectionError("Not connected")

        # Send command
        data = json.dumps(command) + "\n"
        self._socket.sendall(data.encode("utf-8"))

        # Receive response
        while b"\n" not in self._buffer:
            chunk = self._socket.recv(65536)
            if not chunk:
                raise ConnectionError("Connection closed")
            self._buffer += chunk

        line, self._buffer = self._buffer.split(b"\n", 1)
        return json.loads(line.decode("utf-8"))

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
