from src.server import TrackerServer
from src.protocols.Teltonika import Teltonika


if __name__=="__main__":
	TrackerServer(Teltonika.Teltonika)