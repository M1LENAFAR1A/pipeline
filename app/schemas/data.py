from pydantic import BaseModel
from typing import Optional


class DadosBaseSchema(BaseModel):
    class_: Optional[str] = None
    order_: Optional[str] = None
    family: Optional[str] = None
    genus: Optional[str] = None
    species: Optional[str] = None
    subspecies: Optional[str] = None
    latitude: float
    longitude: float
    registration_date: str

# TODO create other schema validations for different database tables
#  at the moment, only dados base is working
