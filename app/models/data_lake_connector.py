from abc import ABC, abstractmethod


class DataLakeConnector(ABC):

    @abstractmethod
    def get_object(self, file_path: str):
        ...

    @abstractmethod
    def upload_data(self, file_path: str, data: bytes, content_type: str, file_size: int):
        ...
