import numpy as np
import pmt
from gnuradio import gr

class blk(gr.sync_block):
    def __init__(self):
        gr.sync_block.__init__(
            self,
            name="Add packet_len tag",
            in_sig=[np.uint8],
            out_sig=[np.uint8]
        )
        self.len_key = pmt.intern("packet_len")

    def work(self, input_items, output_items):
        x = input_items[0]
        y = output_items[0]
        y[:] = x

        tags = self.get_tags_in_window(0, 0, len(x))
        for t in tags:
            if pmt.symbol_to_string(t.key) != "frame_info":
                continue

            info = pmt.to_python(t.value)
            pay_len = info.get("pay_len", None)
            if pay_len is None:
                continue

            # Create a real length tag for Tagged Stream blocks
            self.add_item_tag(
                0,
                t.offset,                       # same offset as frame_info
                self.len_key,
                pmt.from_long(int(pay_len)),
                pmt.intern("")
            )

        return len(x)
