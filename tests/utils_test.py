import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)


import unittest
import struct

from binascii import hexlify

from src.utils import *


class ExtractTest(unittest.TestCase):
	def test_extract(self):
		packet = struct.pack("!bH", 6, 124)
		packet = hexlify(packet)
		packet, ext = extract_byte(packet)
		self.assertEqual(ext, 6)
		self.assertEqual(len(packet), 4)
		packet, ext = extract_short(packet)
		self.assertEqual(ext, 124)
		self.assertEqual(len(packet), 0)


	def test_pack(self):
		values = (2, 2222, 222222, 2.22, 2222.2222, b'abcd')
		packet = ''
		packet = add_byte(packet, values[0])
		packet = add_ushort(packet, values[1])
		packet = add_int(packet, values[2])
		packet = add_float(packet, values[3])
		packet = add_double(packet, values[4])
		packet = add_str(packet, values[5])
		packet = pack(packet)
		test = struct.unpack('!bHifd4s', packet)
		
		for n, value in enumerate(test):
			try:
				self.assertAlmostEqual(value, values[n])
			except Exception:
				self.assertEqual(value, values[n])


	def test_byte(self):
		value = 5
		test_packet = struct.pack("!B", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_byte(test_packet)
		self.assertEqual(test_value, value)


	def test_short(self):
		value = 30000
		test_packet = struct.pack("!H", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_short(test_packet)
		self.assertEqual(test_value, value)


	def test_int(self):
		value = 2000000000
		test_packet = struct.pack("!I", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_int(test_packet)
		self.assertEqual(test_value, value)
	

	def test_longlong(self):
		value = 922337203685477580
		test_packet = struct.pack("!q", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_longlong(test_packet)
		self.assertEqual(test_value, value)


	def test_float(self):
		value = 2221.522
		test_packet = struct.pack("!f", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_float(test_packet)
		self.assertEqual(test_value, value)


	def test_double(self):
		value = 234221.522
		test_packet = struct.pack("!d", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_double(test_packet)
		self.assertEqual(test_value, value)


	def test_ubyte(self):
		value = -5
		test_packet = struct.pack("!b", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_byte(test_packet)
		self.assertEqual(test_value, value)


	def test_ushort(self):
		value = -15000
		test_packet = struct.pack("!h", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_short(test_packet)
		self.assertEqual(test_value, value)


	def test_uint(self):
		value = -1000000000
		test_packet = struct.pack("!i", value)
		test_packet = hexlify(test_packet)
		_, test_value = extract_int(test_packet)
		self.assertEqual(test_value, value)


if __name__=="__main__":
	unittest.main()