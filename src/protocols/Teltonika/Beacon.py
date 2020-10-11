import struct

from enum import Enum
from collections import namedtuple
from typing import List, Dict

from src.utils import *


ibeacon_base_format = '!16s2s2sb'
ibeacon_base_vars = ['UUID', 'Major', 'Minor', 'RSSI']

eddystone_base_format = '!10s6sb'
eddystone_base_vars = ['Namespace', 'InstanceID', 'RSSI']

types = namedtuple('BeaconTypesInfo', ['fmt', 'assign_vars'])


class ParsingError(Exception):
	""" Something goes wrong in parsing """


class UnknownType(ParsingError):
	""" Beacon type is undefined """
	def __init__(self, flag):
		self.message = f'Unknown beacon type ({flag})'
		super().__init__(self.message)


class InsufficientPacketSize(ParsingError):
	""" Packet size insufficiency """
	def __init__(self, size, required_size):
		self.message = f'Have {size//2} bytes but required >={required_size//2} bytes'
		super().__init__(self.message)


class BeaconTypes(Enum):
	iBeacon_RSSI           = types(ibeacon_base_format, ibeacon_base_vars)
	iBeacon_RSSI_BV        = types(ibeacon_base_format+'H', ibeacon_base_vars+['voltage'])
	iBeacon_RSSI_BV_TEMP   = types(ibeacon_base_format+'HH', ibeacon_base_vars+['voltage', 'temp'])
	Eddystone_RSSI         = types(eddystone_base_format, eddystone_base_vars)
	Eddystone_RSSI_BV      = types(eddystone_base_format+'H', eddystone_base_vars+['voltage'])
	Eddystone_RSSI_BV_TEMP = types(eddystone_base_format+'HH', eddystone_base_vars+['voltage', 'temp'])


class BeaconParser:

	@staticmethod
	def parse(packet: bytes) -> List[Dict]:
		all_data = []
		packet, data_part = extract_ubyte(packet)

		while len(packet)>0:
			packet, flag = extract_ubyte(packet)
			beacon_type = BeaconParser.define_beacon_type(flag)

			fmt_size = struct.calcsize(beacon_type.value.fmt)*2
			if len(packet)<fmt_size:
				raise InsufficientPacketSize(len(packet), fmt_size)

			packet, data = BeaconParser.assign_to_vars(packet, beacon_type)
			all_data.append(data)

		return all_data
	

	@staticmethod
	def assign_to_vars(packet: bytes, btype: types) -> dict:
		type_info = btype.value
		size = struct.calcsize(type_info.fmt)
		p = binascii.a2b_hex(packet[:size*2])
		unpacked = struct.unpack(type_info.fmt, p)

		data = []
		for var in unpacked:
			if isinstance(var, bytes):
				data.append(binascii.hexlify(var).decode('ascii'))
			else:
				data.append(var)

		data = dict(zip(type_info.assign_vars, data))
		data["BeaconType"] = btype.name
		return packet[size*2:], data


	@staticmethod
	def define_beacon_type(flag: int) -> types:
		if flag == 33:
			beacon_type = BeaconTypes.iBeacon_RSSI
		elif flag == 35:
			beacon_type = BeaconTypes.iBeacon_RSSI_BV
		elif flag == 39:
			beacon_type = BeaconTypes.iBeacon_RSSI_BV_TEMP
		elif flag == 1:
			beacon_type = BeaconTypes.Eddystone_RSSI
		elif flag == 3:
			beacon_type = BeaconTypes.Eddystone_RSSI_BV
		elif flag == 7:
			beacon_type = BeaconTypes.Eddystone_RSSI_BV_TEMP
		else:
			raise UnknownType(flag)

		return beacon_type

