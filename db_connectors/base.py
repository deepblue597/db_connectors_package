from abc import ABC, abstractmethod
from typing import Any, Dict


class Connector(ABC):
    def __init__(self, address, port):
        self.address = address
        self.port = port
        #self.target = target

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def is_connected(self) -> bool :
        pass

    @abstractmethod
    def get_connection_info(self)-> Dict[str, Any]:
        pass
