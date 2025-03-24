from abc import ABC, abstractmethod


class QueueConnector(ABC):
    def __init__(self, queue: str):
        self.queue = queue
        self.channel = None

    @abstractmethod
    def connect(self):
        ...

    @abstractmethod
    def start_consuming(self):
        ...

    @abstractmethod
    def publish_message(self, message: str):
        ...
