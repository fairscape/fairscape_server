from pydantic import BaseModel, Field
from typing import Union, Optional, Annotated

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
	count: Optional[Union[float, str]] = Field(default=None)
	mean: Optional[Union[float,str]] = Field(default=None)
	std: Optional[Union[float,str]] = Field(default=None)
	min: Optional[Union[float,str]] = Field(default=None)
	first_quartile: Optional[Union[float,str]] = Field(alias="25%", default=None)
	second_quartile: Optional[Union[float,str]] = Field(alias="50%", default=None)
	third_quartile: Optional[Union[float,str]] = Field(alias="75%", default=None)
	max: Optional[Union[float,str]] = Field(default=None)

	def serializeStato(self):
		""" """ 
		pass

class CategoricalStatistics(BaseModel):
	count: Optional[Union[int, str]] = Field(default=None)
	unique: Optional[Union[int, str]] = Field(default=None)
	top: Optional[Union[str,bool]] = Field(default=None)
	freq: Optional[Union[int, str]] = Field(default=None)

	def serializeStato(self):
		""" """ 
		pass

class DescriptiveStatistics(BaseModel):
	columnName: str
	statistics: Union[NumericalStatistics, CategoricalStatistics] 