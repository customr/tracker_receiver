import os
import struct
import binascii
import socket
import threading
import datetime

from time import time, sleep
from json import load, loads, dumps
from enum import Enum

from src.utils import *
from src.db_worker import *
from src.db_connect import CONN
from src.logs.log_config import logger


class ION:

    BASE_PATH = 'tracker_receiver/src/protocols/ION/'
    NAME = 'ION'
    TRACKERS = set()

    def __init__(self, sock, addr, model):
        self.sock = sock
        self.addr = addr
        self.imei = ''


    def start(self):
        ION.TRACKERS.add(self)
        self.db = pymysql.connect(**CONN)

        self.stop = False
        self.data = {}
        self.command_response = {}

        try:
            self.handle_packet()
        except Exception as e:
            self.close()
            raise e


    def close(self):
        ION.TRACKERS.remove(self)
        self.sock.close()
        self.db.close()
        self.stop = True
        logger.info(f'ION {self.imei} отключен [{self.addr[0]}:{self.addr[1]}]')


    def handle_packet(self):
        if self.stop:
            self.stop = False

        while not self.stop:
            packet = binascii.hexlify(self.sock.recv(4096))
                

            if len(packet)==0:
                continue
            
            logger.debug(f'[ION] получен пакет {packet}')
            packet, packet_type = extract_ubyte(packet)
            
            packets = None
            if packet_type==0xe7 or packet_type==0x83:
                packet, packets = extract_ubyte(packet)
            elif packet_type==0xf0 or packet_type==0xf1:
                result = self.handle_command(packet, packet_type)
                resp = {"action":"response", "result": result}
                self.command_response = dumps(resp)

            if not self.imei:
                first_time = True
            else:
                first_time = False

            packet, self.imei = ION.parse_imei(packet)

            if first_time:
                logger.info(f'ION {self.imei} подключен [{self.addr[0]}:{self.addr[1]}]')
                self.assign = get_configuration(self.NAME, self.imei)
                self.ign_v = get_ignition_v(self.imei)
            
            logger.debug(f'[ION] {self.imei} всего записей: {packets}')
            all_data = []
            if packets:
                for i in range(packets):
                    try:
                        packet, data = ION.parse_data(packet)
                        logger.debug(f'[ION] {self.imei} данные #{i} {data}')
                        data = self.rename_data(data)
                        logger.debug(f'[ION] {self.imei} данные после переименования: {data}')
                        data = self.prepare_geo(data)
                        logger.debug(f'[ION] {self.imei} данные после обработки: {data}')
                        all_data.append(data)
                    except Exception as e:
                        logger.error(f'[ION] {self.imei} ошибка парсинга\n{str(e)}\nПакет {packet}')
                        break
            else:
                while len(packet)>0:
                    try:
                        packet, data = ION.parse_data(packet)
                        logger.debug(f'[ION] {self.imei} данные #{i} {data}')
                        data = self.rename_data(data)
                        logger.debug(f'[ION] {self.imei} данные после переименования: {data}')
                        data = self.prepare_geo(data)
                        logger.debug(f'[ION] {self.imei} данные после обработки: {data}')
                        all_data.append(data)
                    except Exception as e:
                        logger.error(f'[ION] {self.imei} ошибка парсинга\n{str(e)}\nПакет {packet}')
                        break

            count = insert_geo(all_data)
            logger.info(f'ION {self.imei} принято {count}/{packets} записей')
            if packet_type==0x82 or packet_type==0x83:
                self.sock.send(b'0')


    @staticmethod
    def parse_imei(packet):
        packet, imei = extract_str(packet, 7)
        imei = binascii.hexlify(imei).decode()
        part_1 = int(f'0x{imei[:6]}', 16)
        part_2 = int(f'0x{imei[6:]}', 16)
        imei = f'{part_1}{part_2}'
        return packet, imei


    @staticmethod
    def parse_data(packet):
        packet, lat = extract_int(packet)
        packet, lon = extract_int(packet)
        packet, speed = extract_ubyte(packet)
        packet, direction = extract_ubyte(packet)
        packet, sat_num = extract_ubyte(packet)
        packet, HDOP = extract_ubyte(packet)
        packet, status = extract_ubyte(packet)

        packet, AIN = extract_ushort(packet)
        packet, VOLTAGE = extract_ushort(packet)
        packet, TEMP = extract_byte(packet)

        packet, day = extract_ubyte(packet)
        packet, month = extract_ubyte(packet)
        packet, year = extract_ubyte(packet)
        packet, hour = extract_ubyte(packet)
        packet, minute = extract_ubyte(packet)
        packet, second = extract_ubyte(packet)
        
        year = int(f'20{year}')
        dt = datetime.datetime(
            day=day, month=month, year=year,
            hour=hour, minute=minute, second=second
            )

        lat = lat/100000
        lon = lon/100000
        lat = lat//100+(lat%100)/60
        lon = lon//100+(lon%100)/60
        speed = speed*1.852
        direction = direction*2
        HDOP = HDOP/10
        VOLTAGE = VOLTAGE/1000

        exclude = ['packet','status','day','month','year','hour','minute','second']
        data = {key:value for key, value in locals().items() if key not in exclude and key!='exclude'}
        return packet, data


    def prepare_geo(self, data):
        ex_keys = ('lat', 'lon', 'speed', 'direction', 'dt')
        reserve = {k:v for k,v in data.items() if k not in ex_keys}
        reserve = str(reserve)[1:-1].replace("'", '"').replace(' ', '')

        geo = {
            'imei': self.imei,
            'lat': float('{:.6f}'.format(data['lat'])),
            'lon': float('{:.6f}'.format(data['lon'])),
            'datetime': data['dt'],
            'type': 0,
            'speed': data['speed'],
            'direction': data['direction'],
            'bat': 0,
            'fuel': 0,
            'ignition': data.get('ignition', 0),
            'sensor': data.get('sensor', 0),
            'reserve': reserve,
            'ts': datetime.datetime.utcfromtimestamp(int(time()))
        }

        return geo


    def rename_data(self, data):
        new_data = {}
        for key, value in data.items():
            if key in self.assign.keys():
                ikey = self.assign[key]
                if '*' in ikey:
                    spl = ikey.split('*')
                    ikey, k = spl[0], spl[1]
                    new_val = round(value*float(k), 2)

                    if ikey=='temp' or ikey=='temp2':
                        new_val = round(new_val, 1)

                    new_data[ikey] = new_val
                    continue

                new_data[ikey] = value
                continue

            new_data[key] = value

        return new_data


    def send_command(self, command):
        packet = ''
        packet = add_ubyte(packet, 0xf0)
        packet = add_ubyte(packet, 0x01)
        packet = add_str(packet, command)
        logger.debug(f'[ION] {self.imei} командный пакет сформирован:\n{packet}\n')
        packet = pack(packet)
        self.sock.send(packet)
        logger.debug(f'[ION] {self.imei} команда {command} отправлена\n')


    def handle_command(self, packet, typ):
        if typ == 0xf0:
            packet, size = extract_ubyte(packet)
        else:
            packet, size = extract_ushort(packet)

        packet, imei = ION.parse_imei(packet)
        packet, _ = extract_byte(packet)
        packet, msg = extract_str(packet, size)

        return msg


    @staticmethod
    def get_tracker(imei):
        for t in ION.TRACKERS:
            if str(t.imei)==str(imei):
                return t

        return None