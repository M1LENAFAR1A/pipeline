from abc import ABC, abstractmethod


class Connector(ABC):

    @abstractmethod
    def upload_data(self, data: str, path: str, content_type: str, data_size: int):
        ...
