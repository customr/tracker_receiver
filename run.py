import os
import threading
import websockets
import asyncio

from time import sleep
from json import load, loads, dumps

from src.server import TrackerServer
from src.protocols.Teltonika import Teltonika
from src.logs.log_config import logger

protocols = {
	'teltonika': Teltonika.Teltonika
}


async def handler():
    uri = "ws://localhost:5678"
    async with websockets.connect(uri) as ws:
    	while True:
	    	try:
	    		rec = await ws.recv()
	    		try:
	    			rec = loads(rec)
	    		except Exception as e:
	    			logger.error(f'WEBSOCKET неизвестный пакет: {rec} {e}\n')
	    			continue
	    		
	    		if rec['action']=='command':
	    			tracker = Teltonika.Teltonika.get_tracker(rec['imei'])
	    			if tracker is not None:
	    				logger.debug(f"WEBSOCKET tracker {rec['imei']} found\n")
	    				Teltonika.Teltonika.send_command(tracker, int(rec['codec']), rec['command'])
	    				logger.info(f"WEBSOCKET {rec['imei']} command {rec['command']} sent")

	    				while tracker.command_response=={}:
	    					sleep(0.1)

	    				logger.debug(f'WEBSOCKET command response\n{tracker.command_response}\n')
	    				try:
	    					await ws.send(tracker.command_response)
	    				except Exception as e:
	    					logger.error(f"WEBSOCKET ошибка при отправке ответа {e}")

	    				tracker.command_response = {}
	    			else:
	    				await ws.send(dumps({"action":"response", "response": "Трекер не подключен"}).encode('ascii'))
	    				logger.info(f"WEBSOCKET {rec['imei']} не подключен")
	    				break

	    		else:
	    			continue

    		except Exception as e:
    			logger.info(e)
    			break



def check_log_size():
	while True:
		if (os.path.getsize('tracker_receiver/src/logs/history.log')/1024) > 1024:
			open('tracker_receiver/src/logs/history.log', 'w').close()

		if (os.path.getsize('tracker_receiver/src/logs/debug.log')/1024) > 1024:
			open('tracker_receiver/src/logs/debug.log', 'w').close()

		sleep(60*60*1)


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
	asyncio.get_event_loop().run_until_complete(handler())