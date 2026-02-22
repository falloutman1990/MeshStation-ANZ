import numpy as np
import math
import pmt
from gnuradio import gr

class blk(gr.sync_block):
    def __init__(self):
        gr.sync_block.__init__(
            self,
            name="Radio Metrics (per packet)",
            in_sig=[np.float32],
            out_sig=[]
        )

        self.message_port_register_in(pmt.intern("frame_info"))
        self.set_msg_handler(pmt.intern("frame_info"), self.handle_frame_info)

        self.message_port_register_in(pmt.intern("payload_done"))
        self.set_msg_handler(pmt.intern("payload_done"), self.handle_payload_done)

        self.message_port_register_out(pmt.intern("out"))

        self.noise_floor = None
        self.measuring = False
        self.sig_sum = 0.0
        self.sig_cnt = 0

        self.packet_id = 0

    def work(self, input_items, output_items):
        power = input_items[0]

        # Update noise floor when NOT measuring a packet
        if not self.measuring:
            avg = float(np.mean(power))
            if self.noise_floor is None:
                self.noise_floor = avg
            else:
                self.noise_floor = 0.99 * self.noise_floor + 0.01 * avg
        else:
            # Accumulate power only during packet
            self.sig_sum += float(np.sum(power))
            self.sig_cnt += int(len(power))

        return len(power)

    def handle_frame_info(self, msg):
        # Start measurement window
        self.measuring = True
        self.sig_sum = 0.0
        self.sig_cnt = 0

    def handle_payload_done(self, msg):
        # End measurement window and emit metrics
        if not self.measuring:
            return

        self.measuring = False

        if self.noise_floor is None or self.sig_cnt == 0:
            return

        ps = self.sig_sum / self.sig_cnt
        pn = self.noise_floor

        if ps <= 0.0 or pn <= 0.0:
            return

        snr = 10.0 * math.log10(ps / pn)
        rssi = 10.0 * math.log10(max(ps, 1e-12))

        self.packet_id += 1
        line = f"RADIOSNR:{snr:.1f},{rssi:.1f},{self.packet_id:08X}\n"
        data = bytearray(line.encode("ascii"))

        pdu = pmt.cons(pmt.PMT_NIL, pmt.init_u8vector(len(data), data))
        self.message_port_pub(pmt.intern("out"), pdu)
