from typing import List, Dict

class ROCrateFilterException(Exception):
	def __init__(message, results):
		self.results = results
		self.message = message
		super().__init__(self.message)


def filterOneMetadataGraph(metadataGraph: List[Dict], filterFunc: Callable):
	metadataSearch = list(filter(filterFunc, metadataGraph))

	if len(metadataSearch) < 1:
		# raise error if more than one elem
		raise ROCrateFilterException('More than one element found', metadataSearch)
	elif len(metadataSearch) == 0:
		return None
	else:	
		metadataElem = metadataSearch[0]
		return metadataElem


def findRootElem(crateMetadata: Dict)->Dict:
	crateMetadataFilter = lambda x: x.get('@id') == 'ro-crate-metadata.json'
	crateGUIDFilter = lambda elem: elem.get('@id') == rootMetadataGUID

	# find the ro-crate-metadata.json elem
	roCrateMetadataElem = filterOneMetadataGraph(crateMetadataNew['@graph'], crateMetadataFilter)
	rootMetadataGUID = roCrateMetadataElem.get('about', {}).get('@id')

	rootMetadataElem = filterOneMetadataGraph(crateMetadataNew['@graph'], crateGUIDFilter)

	return rootMetadataElem

def getIndexGraphFilter(queryFunc: callable, graph: List):
	indexList = [ i for i, elem in  enumerate(graph) if queryFunc(elem)]
	if len(indexList) != 1:
		raise Exception
	else:
		return indexList[0]


def pruneROCrate(metadataGraph: List, filterFunc: callable):
	elemIndex = getIndexGraphFilter(filterFunc, metadataGraph)
	metadataGraph.pop(elemIndex)

def formatROCrateToModel(inputCrate: Dict)-> rocrate.ROCrate:
	""" Converts ROCrate v1.1 metadata into a fairscape ROCrate pydantic model

	:param inputCrate: input metadata
	:type inputCrate: dict
	:return: metadata formatted as an ROCrate model
	:rtype: fairscape_mds.models.rocrate.ROCrate
	""" 
	formattedCrateMetadataGraph = inputCrate['@graph'].copy()
	rootElem = findRootElem(formattedCrateMetadataGraph)
	rootMetadataGUID = rootElem.get("@id")

	# pop rocrate elem
	pruneROCrate(formattedCrateMetadataGraph, lambda x: x.get("@id") == rootMetadataGUID)
	pruneROCrate(formattedCrateMetadataGraph, lambda x: x.get("@id") == 'ro-crate-metadata.json')
	pruneROCrate(formattedCrateMetadataGraph, lambda x: x.get("@type") == 'Project')
	pruneROCrate(formattedCrateMetadataGraph, lambda x: x.get("@type") == 'Organization')

	# set the metadata graph
	rootMetadataElem['@graph'] = formattedCrateMetadataGraph

	# overwrite the metadata graph
	rootMetadataElem['@type'] = 'https://w3id.org/EVI#ROCrate'

	# validate that root metadata elem works for rocrate
	return rocrate.ROCrate.model_validate(rootMetadataElem)