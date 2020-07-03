import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)


import unittest
import struct

from binascii import hexlify

import src.utils


class ExtractTest(unittest.TestCase):
	def test_extract(self):
		packet = struct.pack("!bH", 6, 124)
		packet = hexlify(packet)
		packet, ext = src.utils.extract_byte(packet)
		self.assertEqual(ext, 6)
		self.assertEqual(len(packet), 4)
		packet, ext = src.utils.extract_short(packet)
		self.assertEqual(ext, 124)
		self.assertEqual(len(packet), 0)


	def test_byte(self):
		value = 5
		test_packet = struct.pack("!B", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_byte(test_packet)
		self.assertEqual(test_value, value)


	def test_short(self):
		value = 30000
		test_packet = struct.pack("!H", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_short(test_packet)
		self.assertEqual(test_value, value)


	def test_int(self):
		value = 2000000000
		test_packet = struct.pack("!I", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_int(test_packet)
		self.assertEqual(test_value, value)
	

	def test_longlong(self):
		value = 922337203685477580
		test_packet = struct.pack("!q", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_longlong(test_packet)
		self.assertEqual(test_value, value)


	def test_float(self):
		value = 2221.522
		test_packet = struct.pack("!f", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_float(test_packet)
		self.assertEqual(test_value, value)


	def test_double(self):
		value = 234221.522
		test_packet = struct.pack("!d", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_double(test_packet)
		self.assertEqual(test_value, value)


	def test_ubyte(self):
		value = -5
		test_packet = struct.pack("!b", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_byte(test_packet)
		self.assertEqual(test_value, value)


	def test_ushort(self):
		value = -15000
		test_packet = struct.pack("!h", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_short(test_packet)
		self.assertEqual(test_value, value)


	def test_uint(self):
		value = -1000000000
		test_packet = struct.pack("!i", value)
		test_packet = hexlify(test_packet)
		_, test_value = src.utils.extract_int(test_packet)
		self.assertEqual(test_value, value)


if __name__=="__main__":
	unittest.main()