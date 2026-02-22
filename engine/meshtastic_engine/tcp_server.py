import socket
import threading
import queue


class TCPFrameServer:
    """Send binary frames over a single TCP connection.

    The engine produces already-framed messages (e.g. 0x03 frames). This server
    simply forwards them to a connected client (our GUI). If the client drops,
    it will wait for a new connection.
    """

    def __init__(self, host: str, port: int, q: queue.Queue):
        self.host = host
        self.port = port
        self.q = q
        self._stop = threading.Event()
        self._thread = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)

    def _run(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(1)
        srv.settimeout(0.5)

        client = None
        try:
            while not self._stop.is_set():
                if client is None:
                    try:
                        client, _ = srv.accept()
                        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        client.settimeout(0.5)
                    except socket.timeout:
                        continue

                try:
                    frame = self.q.get(timeout=0.2)
                except queue.Empty:
                    continue

                try:
                    client.sendall(frame)
                except Exception:
                    try:
                        client.close()
                    except Exception:
                        pass
                    client = None
        finally:
            if client:
                try:
                    client.close()
                except Exception:
                    pass
            try:
                srv.close()
            except Exception:
                pass
