﻿import os
import threading
import websockets
import asyncio

from time import sleep
from json import load, loads, dumps

from src.server import TrackerServer
from src.protocols.Teltonika import Teltonika
from src.protocols.Wialon import Wialon
from src.protocols.ADM import ADM
from src.protocols.ION import ION
from src.protocols.WialonCombine import WialonCombine
from src.logs.log_config import logger

protocols = {
	'teltonika': Teltonika.Teltonika,
	'wialon': Wialon.Wialon,
	'adm': ADM.ADM,
	'ion': ION.ION,
	'wialoncombine': WialonCombine.WialonCombine
}


async def handler(ws, path):
	while True:
		try:
			rec = await ws.recv()
			try:
				rec = loads(rec)
			except Exception as e:
				logger.error(f'WEBSOCKET неизвестный пакет: {rec} {e}\n')
				continue

			if rec['action']=='command':
				teltonika = Teltonika.Teltonika.get_tracker(rec['imei'])
				adm = ADM.ADM.get_tracker(rec['imei'])
				ion = ION.ION.get_tracker(rec['imei'])
				wialoncombine = WialonCombine.WialonCombine.get_tracker(rec['imei'])
				if any([teltonika, adm, ion, wialoncombine]):
					command_response = dumps({})
					logger.debug(f"WEBSOCKET tracker {rec['imei']} found")
					if teltonika:
						teltonika.command_response = {}
						teltonika.send_command(int(rec['codec']), rec['command'])
						for _ in range(30):
							sleep(0.5)
							if teltonika.command_response!={}:
								command_response = teltonika.command_response
								break
					elif adm:
						adm.command_response = {}
						adm.send_command(rec['command'])
						for _ in range(30):
							sleep(0.5)
							if adm.command_response!={}:
								command_response = adm.command_response
								break
					elif ion:
						ion.command_response = {}
						ion.send_command(rec['command'])
						for _ in range(30):
							sleep(0.5)
							if ion.command_response!={}:
								command_response = ion.command_response
								break
					elif wialoncombine:
						wialoncombine.command_response = {}
						wialoncombine.send_command(rec['command'])
						for _ in range(30):
							sleep(0.5)
							if wialoncombine.command_response!={}:
								command_response = wialoncombine.command_response
								break
					

					if command_response!={}:
						logger.debug(f'WEBSOCKET command response\n{command_response}')
						try:
							await ws.send(command_response)
						except Exception as e:
							logger.error(f"WEBSOCKET ошибка при отправке ответа {e}")

					else:
						await ws.send(dumps({"action":"response", "result": "Время ожидания ответа истекло"}))

				else:
					await ws.send(dumps({"action":"response", "result": "Трекер не подключен"}))
					logger.info(f"WEBSOCKET {rec['imei']} не подключен")
					continue

			else:
				continue

		except Exception as e:
			raise e



def check_log_size():
	while True:
		if (os.path.getsize('tracker_receiver/src/logs/history.log')/1024) > 1024:
			open('tracker_receiver/src/logs/history.log', 'w').close()

		if (os.path.getsize('tracker_receiver/src/logs/debug.log')/1024) > 1024:
			open('tracker_receiver/src/logs/debug.log', 'w').close()

		sleep(60*60*1)


def start_wsserver():
	start_server = websockets.serve(handler, "127.0.0.1", 5678, ping_interval=None)
	asyncio.get_event_loop().run_until_complete(start_server)
	asyncio.get_event_loop().run_forever()


if __name__=="__main__":
	p = os.path.join('tracker_receiver/src/', 'servers.json')
	with open(p, 'r') as s:
		servers = load(s)

	for protocol, x in servers.items():
		if isinstance(x, dict):
			for model, ipport in x.items():
				TrackerServer(protocols[protocol], ipport, model)

		else:
			TrackerServer(protocols[protocol], x)

	threading.Thread(target=check_log_size).start()
	start_wsserver()