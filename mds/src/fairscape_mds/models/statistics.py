from pydantic import BaseModel, Field
from typing import Union, Optional

# TODO context for STATO values
StatoContext = {
	"count": "",
	"mean": "http://purl.obolibrary.org/obo/STATO_0000573",
	"std": "http://purl.obolibrary.org/obo/STATO_0000684",
	"min":  "http://purl.obolibrary.org/obo/STATO_0000150",
	"first_quartile": "",
	"second_quartile": "",
	"third_quartile": "",
	"max": ""	
}

class NumericalStatistics(BaseModel):
	count: float
	mean: float
	std: float
	min: float
	first_quartile: float = Field(alias="25%")
	second_quartile: float = Field(alias="50%")
	third_quartile: float = Field(alias="75%")
	max: float

	def serializeStato(self):
		""" """ 
		pass

class CategoricalStatistics(BaseModel):
	count: int
	unique: int
	top: Optional[Union[str,bool]] = Field(default=None)
	freq: int

	def serializeStato(self):
		""" """ 
		pass

class DescriptiveStatistics(BaseModel):
	columnName: str
	statistics: Union[NumericalStatistics, CategoricalStatistics] 