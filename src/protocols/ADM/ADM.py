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


class ADM:

    BASE_PATH = 'tracker_receiver/src/protocols/ADM/'
    NAME = 'ADM'
    TRACKERS = set()

    def __init__(self, sock, addr, model):
        self.sock = sock
        self.addr = addr
        self.model = model
        self.need_reply = False
        self.command_response = {}
        self.msg_type = 0


    def start(self):
        ADM.TRACKERS.add(self)
        self.db = pymysql.connect(**CONN)

        self.imei = self.handle_imei()
        logger.info(f'{self.model} {self.imei} подключен [{self.addr[0]}:{self.addr[1]}]')

        self.assign = get_configuration(self.db, self.NAME, self.imei, self.model)
        self.ign_v = get_ignition_v(self.db, self.imei)

        self.lock = threading.Lock()
        self.stop = False
        self.handle_packet()


    def handle_imei(self):
        try:
            packet = binascii.hexlify(self.sock.recv(64))
            packet, device_id = extract_ushort(packet)
            packet, _ = extract_ubyte(packet)
            packet, _ = extract_ubyte(packet)
            packet, imei = extract_str(packet, 15).decode('ascii')
            packet, model = extract_ubyte(packet)
            packet, need_reply = extract_ubyte(packet)
            self.model = Models(model)

            if need_reply==2:
                self.need_reply = True

            logger.debug(f'[{self.model}] imei получен {imei}\n')
            # self.sock.send(struct.pack('!B', 1))
            # logger.debug(f'[ADM] ответ на сообщение с imei отправлен\n')

        except Exception as e:
            ADM.TRACKERS.remove(self)
            self.stop = True
            self.sock.close()
            raise e

        return imei


    def handle_packet(self):
        if self.stop:
            self.stop = False

        while not self.stop:
            self.msg_type = 0
            try:
                packet = binascii.hexlify(self.sock.recv(4096))
            except Exception:
                self.sock.close()
                self.db.close()
                self.stop = True
                logger.info(f'{self.model} {self.imei} отключен [{self.addr[0]}:{self.addr[1]}]')
                break

            self.lock.acquire()
            logger.info(f'packet {packet}')