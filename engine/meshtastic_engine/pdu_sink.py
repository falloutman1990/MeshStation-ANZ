import queue
import pmt
from gnuradio import gr


class PDUSink(gr.basic_block):
    """Message sink that pushes incoming PDUs to a Python queue."""

    def __init__(self, out_queue: queue.Queue):
        gr.basic_block.__init__(self, name="PDUSink", in_sig=None, out_sig=None)
        self._q = out_queue
        self.message_port_register_in(pmt.intern("in"))
        self.set_msg_handler(pmt.intern("in"), self._handle)

    def _handle(self, msg) -> None:
        # Expect PDU: (meta . u8vector)
        if not pmt.is_pair(msg):
            return
        v = pmt.cdr(msg)
        if not pmt.is_u8vector(v):
            return

        data = bytes(bytearray(pmt.u8vector_elements(v)))

        # Non-blocking drop if queue is full
        try:
            self._q.put_nowait(data)
        except queue.Full:
            pass
