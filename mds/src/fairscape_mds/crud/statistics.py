import pandas
import numpy
from typing import Dict
from fairscape_mds.models.statistics import (
	DescriptiveStatistics,
	CategoricalStatistics,
	NumericalStatistics
)


def generateNumericalStatistics(series) -> DescriptiveStatistics:

	descriptiveStats = series.describe()
	descriptiveStats = descriptiveStats.replace({numpy.nan: "NaN"})
	descriptiveStats = descriptiveStats.replace({numpy.inf: "INF"})
	descriptiveStats = descriptiveStats.replace({-numpy.inf: "NINF"})

	numericStats = NumericalStatistics.model_validate(descriptiveStats.to_dict(),by_alias=True)

	return DescriptiveStatistics.model_validate({
		'columnName': descriptiveStats.name,
		'statistics': numericStats
	})


def generateCategoricalStatistics(series) -> DescriptiveStatistics:
	describeSeries = series.describe()

	categoricalDict = describeSeries.to_dict()
	categoricalDict['top'] = categoricalDict.get('top')

	categoricalStats = CategoricalStatistics.model_validate(categoricalDict)

	return DescriptiveStatistics.model_validate({
		'columnName': describeSeries.name,
		'statistics': categoricalStats
	})


def generateSummaryStatistics(dataframe)-> Dict[str, DescriptiveStatistics]:

	
	statistics = {}
	numColumns = dataframe.shape[1]

	for i in range(numColumns):
		series = dataframe.iloc[:, i]

		if pandas.api.types.is_numeric_dtype(series):
			summaryStats = generateNumericalStatistics(series)
		else:
			summaryStats = generateCategoricalStatistics(series)
		
		statistics[summaryStats.columnName] = summaryStats.model_dump(mode='json', by_alias=False)

	return statistics