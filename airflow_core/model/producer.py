from abc import ABC, abstractmethod


class Producer(ABC):
    def __init__(self, channel: str):
        self.channel = channel

    @abstractmethod
    def publish_message(self, message: str):
        ...
