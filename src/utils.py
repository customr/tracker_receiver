import struct
import binascii

from src.logs.log_config import logger

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def extract(packet, length):
	length *= 2
	return packet[length:], packet[:length]

def extract_x(packet, letter, length):
	packet, extracted = extract(packet, length)
	extracted = binascii.a2b_hex(extracted)
	try:
		x = struct.unpack(f"{letter}", extracted)[0]
	except Exception as e:
		logger.critical(f'Ошибка в распаковке: len={length} {letter} {extracted}\n{e}')
		raise e

	return packet, x

def extract_str(packet, length, bdir='!'):
	return extract_x(packet, f'{bdir}{length}s', length)

def extract_byte(packet, bdir='!'):
	return extract_x(packet, f'{bdir}b', 1)

def extract_ubyte(packet, bdir='!'):
	return extract_x(packet, f'{bdir}B', 1)

def extract_short(packet, bdir='!'):
	return extract_x(packet, f'{bdir}h', 2)

def extract_ushort(packet, bdir='!'):
	return extract_x(packet, f'{bdir}H', 2)

def extract_int(packet, bdir='!'):
	return extract_x(packet, f'{bdir}i', 4)

def extract_uint(packet, bdir='!'):
	return extract_x(packet, f'{bdir}I', 4)

def extract_long(packet, bdir='!'):
	return extract_x(packet, f'{bdir}l', 4)

def extract_longlong(packet, bdir='!'):
	return extract_x(packet, f'{bdir}q', 8)

def extract_ulonglong(packet, bdir='!'):
	return extract_x(packet, f'{bdir}Q', 8)

def extract_float(packet, bdir='!'):
	packet, extracted = extract_x(packet, f'{bdir}f', 4)
	return packet, round(extracted, 3)

def extract_double(packet, bdir='!'):
	packet, extracted = extract_x(packet, f'{bdir}d', 8)
	return packet, round(extracted, 3)

def unpack_from_bytes(fmt, packet):
	packet = binascii.a2b_hex(packet)
	return struct.unpack(fmt, packet)



def pack(packet):
	return binascii.a2b_hex(packet)

def add_x(packet, letter, value):
	new_part = binascii.hexlify(struct.pack(f'!{letter}', value)).decode('ascii')
	packet = packet+new_part
	return packet

def add_str(packet, string):
	if not isinstance(string, bytes):
		string = string.encode('ascii')

	return add_x(packet, f'{len(string)}s', string)

def add_byte(packet, value):
	return add_x(packet, 'b', value)

def add_ubyte(packet, value):
	return add_x(packet, 'B', value)

def add_short(packet, value):
	return add_x(packet, 'h', value)

def add_ushort(packet, value):
	return add_x(packet, 'H', value)

def add_int(packet, value):
	return add_x(packet, 'i', value)

def add_uint(packet, value):
	return add_x(packet, 'I', value)

def add_longlong(packet, value):
	return add_x(packet, 'q', value)

def add_float(packet, value):
	return add_x(packet, 'f', value)

def add_double(packet, value):
	return add_x(packet, 'd', value)
