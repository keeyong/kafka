"""
Pydantic is a Python library for data parsing and validation. 
It uses the type hinting mechanism of the newer versions of Python (version 3.6 onwards)
and validates the types during the runtime. Pydantic defines BaseModel class.
It acts as the base class for creating user defined models.
"""
from pydantic import BaseModel


class Person(BaseModel):
    id: str
    name: str
    title: str
