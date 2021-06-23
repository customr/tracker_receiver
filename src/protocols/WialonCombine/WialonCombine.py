import os
import struct
import binascii
import socket
import threading

from datetime import datetime
from time import time, sleep
from json import load, loads, dumps
from enum import Enum

from src.utils import *
from src.db_worker import *
from src.db_connect import CONN
from src.logs.log_config import logger

def crc16(buff, crc =0, poly = 0xa001):
	l = len(buff)
	i = 0
	while i< l:
		ch = buff[i]
		uc = 0
		while uc < 8:
			if (crc & 1) ^ (ch & 1):
				crc = (crc >> 1) ^ poly
			else:
				crc >>= 1
			ch >>= 1
			uc += 1
		i += 1
	return crc


class WialonCombine:

    BASE_PATH = 'tracker_receiver/src/protocols/WialonCombine/'
    NAME = 'WialonCombine'
    TRACKERS = set()

    def __init__(self, sock, addr, model=None):
        self.sock = sock
        self.addr = addr
        self.model = model
        self.stop = True
        self.success = False
        self.imei = ''
        self.command_response = {}

    def start(self):
        logger.info(f'WialonCombine подключен [{self.addr[0]}:{self.addr[1]}]')
        WialonCombine.TRACKERS.add(self)
        self.db = pymysql.connect(**CONN)

        self.pid = 0
        self.stop = False
        self.handle_packet()


    def handle_packet(self):
        if self.stop:
            self.stop = False

        self.assign = None
        while not self.stop:
            try:
                packet = binascii.hexlify(self.sock.recv(2**17))
                self.success = True

            except Exception:
                self.sock.close()
                self.db.close()
                self.stop = True
                self.success = False
                WialonCombine.TRACKERS.remove(self)
                logger.debug(f'[WialonCombine] отключен [{self.addr[0]}:{self.addr[1]}]')
                break

            logger.debug(f'[WialonCombine] получен пакет:\n{packet}\n')
            
            count = 0
            command = False
            packet, packet_header = self.handle_header(packet)
            if packet_header['type']==0:
                try:
                    self.handle_login(packet)
                except:
                    logger.error(f'[WialonCombine] unknown packet\n')
                    continue
                
                logger.debug(f'[WialonCombine] imei:\n{self.imei}\n')
            elif packet_header['type']==1:
                self.assign = get_configuration(self.NAME, self.imei, self.model)
                while len(packet)>4:
                    try:
                        packet, packet_data, packet_reserve = self.handle_data(packet)
                    except Exception as e:
                        logger.debug(f'[WialonCombine] error parsing {packet} {str(e)}\n')
                        break
                    
                    if packet_reserve.get('DRIVERMESSAGE'):
                        command = True
                    else:
                        command = False
                        
                    if not packet_data and packet_reserve:
                        packet_data.append({
                            "lat": 0,
                            "lon": 0,
                            "timestamp": int(time()),
                            "speed": 0,
                            "direction": 0,
                        })
                    for data in packet_data:
                        self.data, _ = self.prepare_data(packet_reserve, data)
                        c = insert_geo([self.data])
                        count += 1
            elif packet_header['type']==2:
                continue
            else:
                logger.debug(f"[WialonCombine] unknown type {packet} {packet_header['type']}\n")
                self.success = False
                
            if self.success:
                resp = ''
                resp = add_short(resp, 0x4040, '>')
                if not command:
                    resp = add_byte(resp, 0x00, '>')
                else:
                    resp = add_byte(resp, 255, '>')
                    
                resp = add_ushort(resp, self.pid, '>')
                logger.debug(f'[WialonCombine] {self.imei} response to tracker:\n{resp}\n')
                resp = pack(resp)
                self.sock.send(resp)
                if count:
                    logger.info(f"WialonCombine {self.imei} принято {count} записи")
            else:
                resp = ''
                resp = add_short(resp, 0x4040, '>')
                resp = add_byte(resp, 0x03, '>')
                resp = add_ushort(resp, self.pid, '>')
                resp = pack(resp)
                self.sock.send(resp)
                logger.warning(f"WialonCombine {self.imei} пакет не был принят")


    def handle_header(self, packet):
        packet, _ = extract_short(packet, '>')
        packet, type = extract_byte(packet, '>')
        packet, self.pid = extract_ushort(packet, '>')
        packet, packet_length = self.get_extended_value_uint(packet)
   
        data = {k:v for k,v in locals().items() if k not in ('self', '_', 'packet', 'extra_len')}
        logger.debug(f'[WialonCombine] header data: {data} {self.pid}')
        return packet, data


    def handle_login(self, packet):
        packet, _ = extract_byte(packet, '>')
        packet, flag = extract_ubyte(packet, '>')
        packet, imei = extract_str(packet, 15, '>')
        packet, pwd = extract_byte(packet, '>')
        packet, crc = extract_ushort(packet, '>')
        self.imei = imei.decode()
        return packet


    def handle_data(self, packet):
        packet, timestamp = extract_uint(packet, '>')
        packet, count = extract_ubyte(packet, '>')
        logger.debug('timestamp '+str(timestamp))
        logger.debug('count '+str(count))
        packet_data = []
        packet_reserve = {}
        for _ in range(count):
            logger.debug('packet '+str(packet))
            logger.debug('packet data '+str(packet_data))
            logger.debug('packet reserve '+str(packet_reserve))
            packet, type = extract_ubyte(packet, '>')
            logger.debug('type '+str(type))
            type = int(type)
            if type==0:
                packet, count = self.get_extended_value_ushort(packet)
                for _ in range(count):
                    logger.debug(f'custom {packet}')
                    packet, name, value = self.handle_custom_param(packet)
                    packet_reserve[name] = value
            elif type==1:
                packet, data = self.handle_pos(packet)
                data['timestamp'] = timestamp
                packet_data.append(data)
            elif type==2:
                packet, data = self.handle_io(packet)
                packet_reserve.update(data)
            elif type==4:
                packet, count = extract_ubyte(packet, '>')
                for _ in range(count):
                    packet, data = self.handle_lbs(packet)
                    packet_reserve.update(data)
            elif type==5:
                packet, count = extract_ubyte(packet, '>')
                for _ in range(count):
                    packet, uid, value = self.handle_param(packet)
                    packet_reserve[f'FUEL{uid}'] = value
            elif type==6:
                packet, count = extract_ubyte(packet, '>')
                for _ in range(count):
                    packet, uid, value = self.handle_param(packet)
                    packet_reserve[f'TEMP{uid}'] = value
            elif type==7:
                packet, count = extract_ubyte(packet, '>')
                for _ in range(count):
                    packet, uid, value = self.handle_param(packet)
                    packet_reserve[f'CAN{uid}'] = value
            elif type==8:
                packet, count = extract_ubyte(packet, '>')
                for _ in range(count):
                    packet, uid, value = self.handle_param(packet)
                    packet_reserve[f'COUNTER{uid}'] = value
            elif type==9:
                packet, count = extract_ubyte(packet, '>')
                for _ in range(count):
                    packet, uid, value = self.handle_param(packet)
                    packet_reserve[f'ADC{uid}'] = value
            elif type==10:
                packet, count = extract_ubyte(packet, '>')
                for _ in range(count):
                    packet, uid, value = self.handle_param(packet)
                    packet_reserve[f'DRIVER{uid}'] = value
            elif type==12:
                packet, msg = extract_str(packet, (len(packet)-6)//2, '!')
                msg = msg.decode()
                resp = {"action":"response", "result": msg}
                self.command_response = dumps(resp)
                packet_reserve[f'DRIVERMESSAGE'] = msg
            else:
                logger.error(f'[WialonCombine] idk')
                self.success = False
                break
            
        logger.debug(f'[WialonCombine] main data: {packet_data} [{packet_reserve}]')
        return packet, packet_data, packet_reserve

    def get_extended_value_short(self, packet):
        _, value = extract_byte(packet, '>')
        if value&0x80==0x80:
            packet, value = extract_short(packet, '>')
            value = value & 0x7f
        else:
            packet, value = extract_byte(packet, '>')
        
        return packet, value
    
    def get_extended_value_ushort(self, packet):
        _, value = extract_ubyte(packet, '>')
        if value&0x80==0x80:
            packet, value = extract_ushort(packet, '>')
            value = value & 0x7f
        else:
            packet, value = extract_ubyte(packet, '>')
        
        return packet, value
    
    def get_extended_value_int(self, packet):
        _, value = extract_short(packet, '>')
        if value&0x8000==0x8000:
            packet, value = extract_int(packet, '>')
            value = value & 0x7fff
        else:
            packet, value = extract_short(packet, '>')
        
        return packet, value
    
    def get_extended_value_uint(self, packet):
        _, value = extract_ushort(packet, '>')
        if value&0x8000==0x8000:
            packet, value = extract_uint(packet, '>')
            value = value & 0x7fff
        else:
            packet, value = extract_ushort(packet, '>')
        
        return packet, value


    def handle_custom_param(self, packet):
        packet, uid, param = self.handle_param(packet)
        name = f'param{uid}'
        return packet, name, param

    def handle_param(self, packet):
        packet, uid = self.get_extended_value_ushort(packet)
        packet, temp = extract_ubyte(packet, '>')
        type_sensor = temp & 0x1f
        divisor_degree = temp>>5
        if type_sensor==0:
            packet, param = extract_ubyte(packet, '>')
        elif type_sensor==1:
            packet, param = extract_ushort(packet, '>')
        elif type_sensor==2:
            packet, param = extract_uint(packet, '>')
        elif type_sensor==3:
            packet, param = extract_ulonglong(packet, '>')
        elif type_sensor==4:
            packet, param = extract_byte(packet, '>')
        elif type_sensor==5:
            packet, param = extract_short(packet, '>')
        elif type_sensor==6:
            packet, param = extract_int(packet, '>')
        elif type_sensor==7:
            packet, param = extract_longlong(packet, '>')
        elif type_sensor==8:
            packet, param = extract_float(packet, '>')
        elif type_sensor==9:
            packet, param = extract_double(packet, '>')
        elif type_sensor==10:
            packet, param = extract_str(packet, packet.find(0x00), '>')
        else:
            raise TypeError('unknown param')
        
        if not type_sensor>=8:
            param = param/(10**divisor_degree)
            
        return packet, uid, param

    def handle_pos(self, packet):
        packet, lat = extract_int(packet, '>')
        lat /= 1000000
        packet, lon = extract_int(packet, '>')
        lon /= 1000000
        packet, speed = extract_ushort(packet, '>')
        packet, direction = extract_ushort(packet, '>')
        packet, height = extract_ushort(packet, '>')
        packet, sat_n = extract_ubyte(packet, '>')
        packet, hdop = extract_ushort(packet, '>')
        
        if speed>255:
            speed=255
            
        if direction>255:
            direction=255
            
        data = {k:v for k,v in locals().items() if k not in ('self', '_', 'packet', 'extra_len')}
        return packet, data

    def handle_lbs(self,packet):
        packet, MCC = extract_short(packet, '>')
        packet, MNC = extract_short(packet, '>')
        packet, LAC = extract_short(packet, '>')
        packet, CellID = extract_short(packet, '>')
        packet, RxLevel = extract_short(packet, '>')
        packet, TA = extract_short(packet, '>')
        data = {k:v for k,v in locals().items() if k not in ('self', '_', 'packet', 'extra_len')}
        return packet, data
    
    def handle_io(self, packet):
        packet, IN_1 = extract_ubyte(packet, '>')
        packet, IN_2 = extract_ubyte(packet, '>')
        packet, IN_3 = extract_ubyte(packet, '>')
        packet, IN_4 = extract_ubyte(packet, '>')
        packet, OUT_1 = extract_ubyte(packet, '>')
        packet, OUT_2 = extract_ubyte(packet, '>')
        packet, OUT_3 = extract_ubyte(packet, '>')
        packet, OUT_4 = extract_ubyte(packet, '>')
        data = {k:v for k,v in locals().items() if k not in ('self', '_', 'packet', 'io')}
        return packet, data
    
    def send_command(self, command):
        packet = ''
        # packet = add_ushort(packet, 0x4040)
        # packet = add_ubyte(packet, 0xFF)
        # packet = add_ushort(packet, len(command)+5)
        # packet = add_uint(packet, int(time()))
        # packet = add_ubyte(packet, 0x00)
        packet = add_str(packet, command)
        # crc16_pack = struct.unpack(f'{len(packet)//2}s', binascii.a2b_hex(packet.encode('ascii')))[0]
        # packet = add_ushort(packet, crc16(crc16_pack))
        # logger.info(f'[WialonCombine] командный пакет сформирован:\n{packet}\n')
        packet = pack(packet)
        try:
            self.sock.send(packet)
            logger.info(f'[WialonCombine]  команда {command} отправлена\n') 
        except Exception as e:
            logger.error(f'[WialonCombine]  команда {command} не отправлена {str(e)}\n') 

    @staticmethod
    def get_tracker(imei):
        for t in WialonCombine.TRACKERS:
            if str(t.imei)==str(imei):
                return t

        return None


    def assign_data(self, data):
        new_data = {}
        for k,v in data.items():
            if self.assign.get(k, None):
                ikey = self.assign[k]
                if '*' in ikey:
                    spl = ikey.split('*')
                    ikey, k = spl[0], spl[1]
                    v = round(v*float(k), 4)

                new_data.update({ikey:v})

            else:
                new_data.update({k:v})

        return new_data


    def prepare_data(self, header, main):
        header['posinfo'] = main
        data = self.assign_data(header)

        if self.model=='v1':
            if data.get('IN_4'):
                if data['IN_4']&0x2:
                    data['ignition'] = 1
                else:
                    data['ignition'] = 0
            else:
                data['ignition'] = 0
        else:
            if data.get('IN_4'):
                if data['IN_4']&0x1:
                    data['ignition'] = 1
                else:
                    data['ignition'] = 0
                    
                if data['IN_4']&0x2:
                    data['IN_0'] = 1
                else:
                    data['IN_0'] = 0
            else:
                data['ignition'] = 0
                

        for k in ('fuel_S', 'RPM', 'temp_4', 'param1', 'param2', 'param3', 'gsm', 'X', 'Y', 'Z', 'odom'):
            if data.get(k):
                data[k] = int(data[k])
        
        if data.get('param1'):
            if data.get('param1')&0x07:
                data['output_1'] = 1
            else:
                data['output_1'] = 0
        
        data['sat_num'] = data.get('param2', 0) + data.get('param3', 0)
        
        if data.get('fuel_used'):
            data['fuel_used'] = round(data['fuel_used'], 1)
        
        insert = 0

        reserve = {}
        for k,v in data.items():
            if k!='posinfo':
                reserve.update({k:v})

        reserve = str(reserve).replace("'", '"')
        reserve = reserve.replace(' ', '')[1:-1]

        if str(self.assign.get('sensor', 'true')).lower() == 'true':
            sensor = data.get('IN_0', 0)
        else:
            sensor = 0   

        if data.get('posinfo'):
            insert = 1
            data = {
                "imei": self.imei,
                "lat": float('{:.6f}'.format(data['posinfo']['lat'])),
                "lon": float('{:.6f}'.format(data['posinfo']['lon'])),
                "datetime": datetime.utcfromtimestamp(data['posinfo']['timestamp']),
                "type": 0,
                "speed": data['posinfo']['speed'],
                "direction": data['posinfo']['direction'],
                "bat": 0,
                "fuel": 0,
                "ignition": data['ignition'],
                "sensor": sensor,
                "reserve": reserve,
                "ts": datetime.utcfromtimestamp(int(time()))
            }

        logger.debug(f'[WialonCombine] final data {data}')
        return data, insert
