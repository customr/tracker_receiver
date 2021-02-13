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

from .tags import TAG, FMT


class GalileoSky:

    BASE_PATH = 'tracker_receiver/src/protocols/GalileoSky/'
    NAME = 'GalileoSky'
    TRACKERS = set()

    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.imei = ''


    def start(self):
        GalileoSky.TRACKERS.add(self)
        self.db = pymysql.connect(**CONN)

        self.imei = self.handle_imei()
        logger.info(f'ADM{self.model} {self.imei} подключен [{self.addr[0]}:{self.addr[1]}]')

        self.assign = get_configuration(self.NAME, self.imei)
        self.ign_v = get_ignition_v(self.imei)

        self.stop = False
        self.data = {}

        try:
            self.handle_packet()
        except Exception as e:
            self.close()
            raise e


    def close(self):
        GalileoSky.TRACKERS.remove(self)
        self.sock.close()
        self.db.close()
        self.stop = True
        logger.info(f'GalileoSky {self.imei} отключен [{self.addr[0]}:{self.addr[1]}]')


    def handle_imei(self):
        try:
            packet = binascii.hexlify(self.sock.recv(1024))
            logger.debug(f'[GalileoSky] пакет с imei: {packet}')

            tag = 0
            while tag!=0x03:
                packet, tag = extract_ubyte(packet, '<')

            packet, imei = extract_str(packet, 15)
            # self.sock.send(struct.pack('!B', 1))
            # logger.debug(f'[GalileoSky] ответ на сообщение с imei отправлен\n')

        except Exception as e:
            self.close()
            raise e

        return imei.decode('ascii')


    def handle_packet(self):
        if self.stop:
            self.stop = False

        while not self.stop:
            try:
                packet = binascii.hexlify(self.sock.recv(1024))
            except Exception as e:
                self.close()
                break

            if len(packet)==0:
                continue

            logger.debug(f'[GalileoSky] {self.imei} принят пакет {packet}')
            all_data = []
            while len(packet)>0:
                packet, data = self.data_main(packet)
                if packet is None:
                    break
