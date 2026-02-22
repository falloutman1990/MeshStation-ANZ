#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# SPDX-License-Identifier: GPL-3.0
#
# GNU Radio Python Flow Graph
# Title: rx_lora_base_engine
# Author: IronGiu (Base offered by: Josh Conway )
# Description: This flow is a base for meshtastic engine of MeshStation by irongiu , thus only needing a RTL-SDR.
# GNU Radio version: 3.10.12.0

from gnuradio import blocks
from gnuradio import filter
from gnuradio.filter import firdes
from gnuradio import gr
from gnuradio.fft import window
import sys
import signal
from argparse import ArgumentParser
from gnuradio.eng_arg import eng_float, intx
from gnuradio import eng_notation
from gnuradio import gr, pdu
import gnuradio.lora_sdr as lora_sdr
import numpy as np
import osmosdr
import time
from . import rx_lora_base_engine_epy_block_0 as epy_block_0  # embedded python block
from . import rx_lora_base_engine_epy_block_1 as epy_block_1  # embedded python block
from . import rx_lora_base_engine_epy_block_2 as epy_block_2  # embedded python block
import threading




class rx_lora_base_engine(gr.top_block):

    def __init__(
        self,
        center_freq=869525000,
        samp_rate=1000000,
        lora_bw=250000,
        sf=9,
        gain=30,
        ppm=69,
        if_gain=20,
        bb_gain=20,
        preamble_length=17,
        payload_length=237,
        impl_head=False,
        has_crc=True,
        sync_word=(0, 0),
        device_args="",
    ):
        gr.top_block.__init__(self, "rx_lora_base_engine", catch_exceptions=True)
        self.flowgraph_started = threading.Event()

        ##################################################
        # Variables
        ##################################################
        samp_rate = int(samp_rate)
        lora_bw = int(lora_bw)
        sync_word = [int(x) for x in sync_word]
        self.sync_word = sync_word
        self.soft_decoding = soft_decoding = True
        sf = int(sf)
        preamble_length = int(preamble_length)
        ppm = int(ppm)
        self.payload_length = payload_length
        self.impl_head = impl_head
        self.has_crc = has_crc
        self.gain = gain
        self.cr_48 = cr_48 = 8
        self.cr_47 = cr_47 = 3
        self.cr_46 = cr_46 = 2
        self.cr_45 = cr_45 = 1
        self.cr_44 = cr_44 = 0
        center_freq = int(center_freq)
        self.bandpass250k = bandpass250k = firdes.complex_band_pass(1.0, samp_rate, -lora_bw/2, lora_bw/2, lora_bw/10, window.WIN_HAMMING, 6.76)

        ##################################################
        # Blocks
        ##################################################

        self.rtlsdr_source_0 = osmosdr.source(
            args="numchan=" + str(1) + " " + str(device_args)
        )
        self.rtlsdr_source_0.set_time_source('external', 0)
        self.rtlsdr_source_0.set_time_unknown_pps(osmosdr.time_spec_t())
        self.rtlsdr_source_0.set_sample_rate(samp_rate)
        self.rtlsdr_source_0.set_center_freq(center_freq, 0)
        self.rtlsdr_source_0.set_freq_corr(ppm, 0)
        self.rtlsdr_source_0.set_dc_offset_mode(0, 0)
        self.rtlsdr_source_0.set_iq_balance_mode(0, 0)
        self.rtlsdr_source_0.set_gain_mode(False, 0)
        self.rtlsdr_source_0.set_gain(gain, 0)
        self.rtlsdr_source_0.set_if_gain(int(if_gain), 0)
        self.rtlsdr_source_0.set_bb_gain(int(bb_gain), 0)
        self.rtlsdr_source_0.set_antenna('', 0)
        self.rtlsdr_source_0.set_bandwidth(0, 0)
        self.pdu_tagged_stream_to_pdu_0 = pdu.tagged_stream_to_pdu(gr.types.byte_t, 'packet_len')
        self.lora_sdr_header_decoder_0_1 = lora_sdr.header_decoder(impl_head, cr_45, payload_length, has_crc, 2, False)
        self.lora_sdr_hamming_dec_0_1 = lora_sdr.hamming_dec(soft_decoding)
        self.lora_sdr_gray_mapping_0_1 = lora_sdr.gray_mapping( soft_decoding)
        self.lora_sdr_frame_sync_0_1 = lora_sdr.frame_sync(center_freq, lora_bw, sf, impl_head, sync_word, 4,preamble_length)
        self.lora_sdr_fft_demod_0_1 = lora_sdr.fft_demod( soft_decoding, True)
        self.lora_sdr_dewhitening_0_1 = lora_sdr.dewhitening()
        self.lora_sdr_deinterleaver_0_1 = lora_sdr.deinterleaver( soft_decoding)
        self.lora_sdr_crc_verif_0_1 = lora_sdr.crc_verif( 2, False)
        self.freq_xlating_fir_filter_xxx_0 = filter.freq_xlating_fir_filter_ccc((max(1, int(samp_rate/(lora_bw * 4)))), bandpass250k, 0, samp_rate)
        self.freq_xlating_fir_filter_xxx_0.set_min_output_buffer(17000)
        self.epy_block_2 = epy_block_2.blk()
        self.epy_block_1 = epy_block_1.blk()
        self.aggregator = self.epy_block_1  # expose for engine
        self.epy_block_0 = epy_block_0.blk()
        self.blocks_moving_average_xx_0 = blocks.moving_average_ff(1024, (1.0 / 1024), 4000, 1)
        self.blocks_complex_to_mag_squared_0 = blocks.complex_to_mag_squared(1)


        ##################################################
        # Connections
        ##################################################
        self.msg_connect((self.epy_block_0, 'out'), (self.epy_block_1, 'metrics'))
        self.msg_connect((self.lora_sdr_crc_verif_0_1, 'msg'), (self.epy_block_0, 'payload_done'))
        self.msg_connect((self.lora_sdr_header_decoder_0_1, 'frame_info'), (self.epy_block_0, 'frame_info'))
        self.msg_connect((self.lora_sdr_header_decoder_0_1, 'frame_info'), (self.lora_sdr_frame_sync_0_1, 'frame_info'))
        self.msg_connect((self.pdu_tagged_stream_to_pdu_0, 'pdus'), (self.epy_block_1, 'payload'))
        self.connect((self.blocks_complex_to_mag_squared_0, 0), (self.blocks_moving_average_xx_0, 0))
        self.connect((self.blocks_moving_average_xx_0, 0), (self.epy_block_0, 0))
        self.connect((self.epy_block_2, 0), (self.pdu_tagged_stream_to_pdu_0, 0))
        self.connect((self.freq_xlating_fir_filter_xxx_0, 0), (self.blocks_complex_to_mag_squared_0, 0))
        self.connect((self.freq_xlating_fir_filter_xxx_0, 0), (self.lora_sdr_frame_sync_0_1, 0))
        self.connect((self.lora_sdr_crc_verif_0_1, 0), (self.epy_block_2, 0))
        self.connect((self.lora_sdr_deinterleaver_0_1, 0), (self.lora_sdr_hamming_dec_0_1, 0))
        self.connect((self.lora_sdr_dewhitening_0_1, 0), (self.lora_sdr_crc_verif_0_1, 0))
        self.connect((self.lora_sdr_fft_demod_0_1, 0), (self.lora_sdr_gray_mapping_0_1, 0))
        self.connect((self.lora_sdr_frame_sync_0_1, 0), (self.lora_sdr_fft_demod_0_1, 0))
        self.connect((self.lora_sdr_gray_mapping_0_1, 0), (self.lora_sdr_deinterleaver_0_1, 0))
        self.connect((self.lora_sdr_hamming_dec_0_1, 0), (self.lora_sdr_header_decoder_0_1, 0))
        self.connect((self.lora_sdr_header_decoder_0_1, 0), (self.lora_sdr_dewhitening_0_1, 0))
        self.connect((self.rtlsdr_source_0, 0), (self.freq_xlating_fir_filter_xxx_0, 0))


    def get_samp_rate(self):
        return self.samp_rate

    def set_samp_rate(self, samp_rate):
        self.samp_rate = samp_rate
        self.set_bandpass250k(firdes.complex_band_pass(1.0, self.samp_rate, -self.lora_bw/2, self.lora_bw/2, self.lora_bw/10, window.WIN_HAMMING, 6.76))
        self.rtlsdr_source_0.set_sample_rate(self.samp_rate)

    def get_lora_bw(self):
        return self.lora_bw

    def set_lora_bw(self, lora_bw):
        self.lora_bw = lora_bw
        self.set_bandpass250k(firdes.complex_band_pass(1.0, self.samp_rate, -self.lora_bw/2, self.lora_bw/2, self.lora_bw/10, window.WIN_HAMMING, 6.76))

    def get_sync_word(self):
        return self.sync_word

    def set_sync_word(self, sync_word):
        self.sync_word = sync_word

    def get_soft_decoding(self):
        return self.soft_decoding

    def set_soft_decoding(self, soft_decoding):
        self.soft_decoding = soft_decoding

    def get_sf(self):
        return self.sf

    def set_sf(self, sf):
        self.sf = sf

    def get_preamble_length(self):
        return self.preamble_length

    def set_preamble_length(self, preamble_length):
        self.preamble_length = preamble_length

    def get_ppm(self):
        return self.ppm

    def set_ppm(self, ppm):
        self.ppm = ppm
        self.rtlsdr_source_0.set_freq_corr(self.ppm, 0)

    def get_payload_length(self):
        return self.payload_length

    def set_payload_length(self, payload_length):
        self.payload_length = payload_length

    def get_impl_head(self):
        return self.impl_head

    def set_impl_head(self, impl_head):
        self.impl_head = impl_head

    def get_has_crc(self):
        return self.has_crc

    def set_has_crc(self, has_crc):
        self.has_crc = has_crc

    def get_gain(self):
        return self.gain

    def set_gain(self, gain):
        self.gain = gain
        self.rtlsdr_source_0.set_gain(self.gain, 0)

    def get_cr_48(self):
        return self.cr_48

    def set_cr_48(self, cr_48):
        self.cr_48 = cr_48

    def get_cr_47(self):
        return self.cr_47

    def set_cr_47(self, cr_47):
        self.cr_47 = cr_47

    def get_cr_46(self):
        return self.cr_46

    def set_cr_46(self, cr_46):
        self.cr_46 = cr_46

    def get_cr_45(self):
        return self.cr_45

    def set_cr_45(self, cr_45):
        self.cr_45 = cr_45

    def get_cr_44(self):
        return self.cr_44

    def set_cr_44(self, cr_44):
        self.cr_44 = cr_44

    def get_center_freq(self):
        return self.center_freq

    def set_center_freq(self, center_freq):
        self.center_freq = center_freq
        self.rtlsdr_source_0.set_center_freq(self.center_freq, 0)

    def get_bandpass250k(self):
        return self.bandpass250k

    def set_bandpass250k(self, bandpass250k):
        self.bandpass250k = bandpass250k
        self.freq_xlating_fir_filter_xxx_0.set_taps(self.bandpass250k)




def build_top_block(**kwargs):
    """Factory for the engine top block."""
    return rx_lora_base_engine(**kwargs)


def main(top_block_cls=rx_lora_base_engine, options=None):
    tb = top_block_cls()

    def sig_handler(sig=None, frame=None):
        tb.stop()
        tb.wait()

        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    tb.start()
    tb.flowgraph_started.set()

    try:
        input('Press Enter to quit: ')
    except EOFError:
        pass
    tb.stop()
    tb.wait()


if __name__ == '__main__':
    main()
