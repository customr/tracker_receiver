from emum import Enum
from src.utils import *

class FMT(Enum):
    imei = extract_str

class TAG(Enum):
    imei = 0x03
