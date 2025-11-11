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
	count: Optional[float] = Field(allow_inf_nan=True, default=None)
	mean: Optional[float] = Field(allow_inf_nan=True, default=None)
	std: Optional[float] = Field(allow_inf_nan=True, default=None)
	min: Optional[float] = Field(allow_inf_nan=True, default=None)
	first_quartile: Optional[float] = Field(alias="25%", allow_inf_nan=True, default=None)
	second_quartile: Optional[float] = Field(alias="50%", allow_inf_nan=True, default=None)
	third_quartile: Optional[float] = Field(alias="75%", allow_inf_nan=True, default=None)
	max: Optional[float] = Field(allow_inf_nan=True, default=None)

	def serializeStato(self):
		""" """ 
		pass

class CategoricalStatistics(BaseModel):
	count: Optional[int] = Field(allow_inf_nan=True, default=None)
	unique: Optional[int] = Field(allow_inf_nan=True, default=None)
	top: Optional[Union[str,bool]] = Field(default=None)
	freq: Optional[int] = Field(allow_inf_nan=True, default=None)

	def serializeStato(self):
		""" """ 
		pass

class DescriptiveStatistics(BaseModel):
	columnName: str
	statistics: Union[NumericalStatistics, CategoricalStatistics] 