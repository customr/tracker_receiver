import os
import threading

from time import sleep

from src.server import TrackerServer
from src.protocols.Teltonika import Teltonika


def check_log_size():
	while True:
		if (os.path.getsize('tracker_receiver/src/logs/history.log')/1024) > 1024*1024:
			open('tracker_receiver/src/logs/history.log', 'w').close()
		
		sleep(60*60*4)

if __name__=="__main__":
	TrackerServer(Teltonika.Teltonika)
	threading.Thread(target=check_log_size).start()