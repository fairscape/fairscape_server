import pandas
from typing import Dict
from fairscape_mds.models.statistics import (
	DescriptiveStatistics,
	CategoricalStatistics,
	NumericalStatistics
)


def generateNumericalStatistics(series) -> DescriptiveStatistics:

	descriptiveStats = series.describe()

	numericStats = NumericalStatistics.model_validate(descriptiveStats.to_dict(),by_alias=True)

	return DescriptiveStatistics.model_validate({
		'columnName': descriptiveStats.name,
		'statistics': numericStats
	})


def generateCategoricalStatistics(series) -> DescriptiveStatistics:
	describeSeries = series.describe()

	categoricalDict = describeSeries.to_dict()

	if categoricalDict.get('top') is None:
		categoricalDict['top'] = None

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
		
		statistics[summaryStats.columnName] = summaryStats

	return statistics