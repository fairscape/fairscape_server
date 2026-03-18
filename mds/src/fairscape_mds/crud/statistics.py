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


def generateNumericalStatistics(series, bin_edges=None) -> DescriptiveStatistics:

	descriptiveStats = series.describe()
	descriptiveStats = descriptiveStats.replace({numpy.nan: "NaN"})
	descriptiveStats = descriptiveStats.replace({numpy.inf: "INF"})
	descriptiveStats = descriptiveStats.replace({-numpy.inf: "NINF"})

	descriptiveStatsDict = descriptiveStats.to_dict()

	# missing data
	missing_count = int(series.isna().sum())
	total = len(series)
	missing_percentage = round((missing_count / total) * 100, 2) if total > 0 else 0.0

	# histogram on non-null values
	clean = series.dropna()
	histogram_bins = None
	histogram_counts = None
	if len(clean) > 0:
		bins_arg = bin_edges if bin_edges is not None else 10
		counts, computed_edges = numpy.histogram(clean, bins=bins_arg)
		histogram_counts = counts.tolist()
		histogram_bins = computed_edges.tolist()

	numericStats = NumericalStatistics.model_validate(
			{
				'count': descriptiveStatsDict['count'],
				'mean': descriptiveStatsDict['mean'],
				'std': descriptiveStatsDict['std'],
				'min': descriptiveStatsDict['min'],
				'first_quartile': descriptiveStatsDict['25%'],
				'second_quartile': descriptiveStatsDict['50%'],
				'third_quartile': descriptiveStatsDict['75%'],
				'max': descriptiveStatsDict['max'],
				'missing_count': missing_count,
				'missing_percentage': missing_percentage,
				'histogram_bins': histogram_bins,
				'histogram_counts': histogram_counts,
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

	# missing data
	missing_count = int(series.isna().sum())
	total = len(series)
	categoricalDict['missing_count'] = missing_count
	categoricalDict['missing_percentage'] = round((missing_count / total) * 100, 2) if total > 0 else 0.0

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


def collectHistogramBins(statistics: Dict) -> Dict[str, list]:
	"""Extract histogram bin edges from total statistics for reuse in splits."""
	bin_edges = {}
	for col_name, col_data in statistics.items():
		bins = col_data.get('statistics', {}).get('histogram_bins')
		if bins is not None:
			bin_edges[col_name] = bins
	return bin_edges


def generateSummaryStatisticsWithBins(
	dataframe, totalBinEdges: Dict[str, list]
) -> Dict[str, DescriptiveStatistics]:
	"""Like generateSummaryStatistics but uses pre-computed bin edges for histograms."""
	if dataframe.shape[1] > descriptiveStatisticsMaxCols:
		return {}

	statistics = {}
	numColumns = dataframe.shape[1]

	for i in range(numColumns):
		series = dataframe.iloc[:, i]

		if pandas.api.types.is_numeric_dtype(series):
			edges = totalBinEdges.get(series.name)
			summaryStats = generateNumericalStatistics(series, bin_edges=edges)
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
	splits: List,
	totalBinEdges: Optional[Dict[str, list]] = None
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
				if totalBinEdges:
					stats = generateSummaryStatisticsWithBins(subset, totalBinEdges)
				else:
					stats = generateSummaryStatistics(subset)
				splitStats[name] = {
					"query": query,
					"queryType": queryType,
					"description": description,
					"statistics": stats
				}
		except Exception:
			# Skip splits that fail to query
			continue

	return splitStats