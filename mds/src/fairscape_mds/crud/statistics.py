import pandas
import numpy
from typing import Dict, List, Optional
from fairscape_mds.core.config import descriptiveStatisticsMaxCols
from fairscape_mds.models.statistics import (
	DescriptiveStatistics,
	CategoricalStatistics,
	NumericalStatistics
)

try:
	import pandasql
except ImportError:
	pandasql = None


def generateNumericalStatistics(series) -> DescriptiveStatistics:

	descriptiveStats = series.describe()
	descriptiveStats = descriptiveStats.replace({numpy.nan: "NaN"})
	descriptiveStats = descriptiveStats.replace({numpy.inf: "INF"})
	descriptiveStats = descriptiveStats.replace({-numpy.inf: "NINF"})

	descriptiveStatsDict = descriptiveStats.to_dict()

	numericStats = NumericalStatistics.model_validate(
			{
				'count': descriptiveStatsDict['count'],
				'mean': descriptiveStatsDict['mean'],
				'std': descriptiveStatsDict['std'],
				'min': descriptiveStatsDict['min'],
				'first_quartile': descriptiveStatsDict['25%'],
				'second_quartile': descriptiveStatsDict['50%'],
				'third_quartile': descriptiveStatsDict['75%'],
				'max': descriptiveStatsDict['max']
			}	
		)

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

	# if too many columns
	if dataframe.shape[1] > descriptiveStatisticsMaxCols:
		return {}

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


def applyQuery(dataframe: pandas.DataFrame, query: str, queryType: str) -> Optional[pandas.DataFrame]:
	queryType = queryType.upper() if queryType else None

	if queryType == "SQL":
		if pandasql is None:
			raise ImportError("pandasql is required for SQL split queries. Install it with: pip install pandasql")
		dataset = dataframe  # noqa: F841 — referenced by SQL query
		return pandasql.sqldf(query, locals())

	elif queryType == "PANDAS":
		# Extract the condition from queries like "split == 'train'"
		return dataframe.query(query)

	else:
		return None


def generateSplitStatistics(
	dataframe: pandas.DataFrame,
	splits: List
) -> Dict[str, Dict]:
	splitStats = {}

	for split in splits:
		_get = lambda k: split.get(k) if isinstance(split, dict) else getattr(split, k, None)
		query = _get("query")
		queryType = _get("queryType")
		name = _get("name")
		description = _get("description")

		if not query or not queryType or not name:
			continue

		try:
			subset = applyQuery(dataframe, query, queryType)
			if subset is not None and not subset.empty:
				splitStats[name] = {
					"query": query,
					"queryType": queryType,
					"description": description,
					"statistics": generateSummaryStatistics(subset)
				}
		except Exception:
			# Skip splits that fail to query
			continue

	return splitStats