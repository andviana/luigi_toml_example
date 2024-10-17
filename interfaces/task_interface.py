from abc import ABC, abstractmethod
from config.config import Config
import luigi

class TaskInterface(ABC):
    @abstractmethod
    def output(self):
        pass

    @abstractmethod
    def run(self):
        pass

    def get_config(self):
        config = Config().get_config()
        return config