from typing import Union

from pydantic import BaseModel


class Response(BaseModel):
    success: bool
    value: str


class MinioResponse(BaseModel):
    success: bool
    value: Union[str, bytes]
