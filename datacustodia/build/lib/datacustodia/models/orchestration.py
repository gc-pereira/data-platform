from pydantic import BaseModel, Field, validator
from typing import List, Optional, Literal


class PredicateModel(BaseModel):
    PushDown: str
    Type: Literal["Date", "String", "Numeric"]
    Filter: int
    Pattern: Optional[str] = None


class DependencyQualityModel(BaseModel):
    Rules: List[str]
    ReadIfFail: bool


class DependencyModel(BaseModel):
    TableName: str
    DatabaseName: str
    Predicate: List[PredicateModel]
    Quality: DependencyQualityModel


class MachineModel(BaseModel):
    Capacity: int
    Type: Literal["G.1X", "G.2X"]


class MainQualityModel(BaseModel):
    Rules: List[str]
    WriteIfFail: bool


class ConfigModel(BaseModel):
    DataLayer: Literal["SOR", "SOT", "SPEC", "CSV"]
    TableName: str
    DatabaseName: str
    Machine: MachineModel
    Dependencies: Optional[List[DependencyModel]] = Field(default_factory=list)
    Quality: MainQualityModel
