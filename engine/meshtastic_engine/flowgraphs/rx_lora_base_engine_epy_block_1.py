import pmt
import time
from gnuradio import gr

class blk(gr.basic_block):
    def __init__(self):
        gr.basic_block.__init__(self, name="Frame Aggregator (unified)", in_sig=None, out_sig=None)

        self.message_port_register_in(pmt.intern("payload"))
        self.message_port_register_in(pmt.intern("metrics"))
        self.message_port_register_out(pmt.intern("out"))

        self.set_msg_handler(pmt.intern("payload"), self.handle_payload)
        self.set_msg_handler(pmt.intern("metrics"), self.handle_metrics)

        # Queues
        self.metrics_q = []   # list of (ts, snr10, rssi10)
        # Pending payload (only need 1 pending at a time for minimal latency)
        self.pending_payload = None  # (ts, payload_bytes)
        self.pending_deadline = 0.0

        # Tuning
        self.payload_wait_ms = 30     # wait this long for a metrics to arrive
        self.metrics_ttl_ms = 200     # drop metrics if no payload within TTL
        self.max_metrics_q = 200

    def _u8_from_any(self, msg):
        # PDU: (meta . u8vector)
        if pmt.is_pair(msg):
            v = pmt.cdr(msg)
            if pmt.is_u8vector(v):
                return bytearray(pmt.u8vector_elements(v))
            return None

        # Raw u8vector
        if pmt.is_u8vector(msg):
            return bytearray(pmt.u8vector_elements(msg))

        # Metrics may already be bytes PDU; otherwise ignore
        return None

    def _cleanup_metrics(self, now):
        ttl = self.metrics_ttl_ms / 1000.0
        # Drop old metrics
        while self.metrics_q and (now - self.metrics_q[0][0]) > ttl:
            self.metrics_q.pop(0)

        # Cap queue
        if len(self.metrics_q) > self.max_metrics_q:
            self.metrics_q = self.metrics_q[-self.max_metrics_q:]

    def _encode_i16(self, x):
        # x must be in [-32768, 32767]
        x = int(max(-32768, min(32767, x)))
        return bytes([(x >> 8) & 0xFF, x & 0xFF])

    def _emit_unified(self, payload_bytes, metrics):
        # metrics: None or (snr10, rssi10)
        pl = len(payload_bytes)
        if pl > 65535:
            payload_bytes = payload_bytes[:65535]
            pl = 65535

        flags = 0x01 if metrics is not None else 0x00
        if metrics is None:
            snr10, rssi10 = 0, 0
        else:
            snr10, rssi10 = metrics

        body = bytearray()
        # payload_len
        body.append((pl >> 8) & 0xFF)
        body.append(pl & 0xFF)
        body.extend(payload_bytes)
        # flags + metrics
        body.append(flags)
        body.extend(self._encode_i16(snr10))
        body.extend(self._encode_i16(rssi10))

        # Frame header: type=0x03 + len
        ln = len(body)
        frame = bytearray([0x03, (ln >> 8) & 0xFF, ln & 0xFF])
        frame.extend(body)

        pdu = pmt.cons(pmt.PMT_NIL, pmt.init_u8vector(len(frame), frame))
        self.message_port_pub(pmt.intern("out"), pdu)

    def handle_metrics(self, msg):
        now = time.time()
        self._cleanup_metrics(now)

        # Metrics arrive as ASCII PDU from your metrics block; parse quickly
        # We accept either u8vector bytes or PDU bytes
        raw = self._u8_from_any(msg)
        if raw is None:
            # Try symbol/string as last resort without breaking on binary
            try:
                b = bytearray(pmt.serialize_str(msg))
                raw = b
            except Exception:
                return

        # Expect "RADIOSNR:<snr>,<rssi>,..."
        try:
            txt = raw.decode("ascii", errors="ignore").strip()
            if not txt.startswith("RADIOSNR:"):
                return
            payload = txt.split(":", 1)[1]
            parts = [p.strip() for p in payload.split(",")]
            if len(parts) < 2:
                return
            snr = float(parts[0])
            rssi = float(parts[1])
            snr10 = int(round(snr * 10.0))
            rssi10 = int(round(rssi * 10.0))
        except Exception:
            return

        self.metrics_q.append((now, snr10, rssi10))
        self._cleanup_metrics(now)

        # If a payload is waiting, flush immediately with freshest metrics
        if self.pending_payload is not None:
            ts, pb = self.pending_payload
            # use newest metric
            m = self.metrics_q.pop(-1) if self.metrics_q else None
            metrics = (m[1], m[2]) if m else None
            self._emit_unified(pb, metrics)
            self.pending_payload = None
            self.pending_deadline = 0.0

    def handle_payload(self, msg):
        now = time.time()
        self._cleanup_metrics(now)

        payload_bytes = self._u8_from_any(msg)
        if payload_bytes is None:
            # If it's a PMT string-like, it's safer to reject than corrupt payload
            return

        # If we already have a pending payload, flush it with null (avoid buildup)
        if self.pending_payload is not None and now >= self.pending_deadline:
            _, pb_old = self.pending_payload
            self._emit_unified(pb_old, None)
            self.pending_payload = None
            self.pending_deadline = 0.0

        # If metrics available, pair immediately (FIFO-ish but uses newest, low latency)
        if self.metrics_q:
            m = self.metrics_q.pop(0)
            self._emit_unified(payload_bytes, (m[1], m[2]))
            return

        # Otherwise, wait a tiny bit for metrics; if none arrives, send null
        self.pending_payload = (now, payload_bytes)
        self.pending_deadline = now + (self.payload_wait_ms / 1000.0)

        # If we cannot wait (e.g., wait is 0), flush now
        if self.payload_wait_ms <= 0:
            _, pb = self.pending_payload
            self._emit_unified(pb, None)
            self.pending_payload = None
            self.pending_deadline = 0.0
