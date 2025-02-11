import urllib
import json
from typing import (
	List,
	Dict,
	Callable
)
from fairscape_mds.models.rocrate import (
	ROCrate,
	ROCrateV1_2,
	ROCrateDataset,
	ROCrateComputation,
	ROCrateSoftware
)

from fairscape_mds.rocrate.errors import (
	ROCrateFilterException
)


def filterOneMetadataGraph(metadataGraph: List[Dict], filterFunc: Callable) -> Dict:	
	""" Given a metadataGraph filter the elements using a passed callable and return one element 
	:param metadataGraph: input metadata graph
	:type metadataGraph: List[Dict]
	:param filterFunc: filter function to preform on the graph
	:type filterFunc: Callable
	:rtype: Dict | None
	"""
	metadataSearch = list(filter(filterFunc, metadataGraph))

	if len(metadataSearch) == 1:
		metadataElem = metadataSearch[0]
		return metadataElem
	else:
		if len(metadataSearch)>1:
			# raise error if more than one elem
			raise ROCrateFilterException('More than one element found', metadataSearch)
		else: 
			raise ROCrateFilterException('No matching element found', metadataSearch)


def findRootElem(crateMetadata: Dict)->dict:
	""" Filter ROCrate metadata to find the root element representing the toplevel ROCrate 

	:param crateMetadata: input metadata
	:type crateMetadata: dict
	:return: Root ROCrate Element
	:rtype: Dict
	"""
	crateMetadataFilter = lambda x: x.get('@id') == 'ro-crate-metadata.json'

	# find the ro-crate-metadata.json elem
	roCrateMetadataElem = filterOneMetadataGraph(crateMetadata['@graph'], crateMetadataFilter)
	if roCrateMetadataElem is None:
		raise Exception("root elem not found")
		
	rootMetadataGUID = roCrateMetadataElem.get('about', {}).get('@id')

	if rootMetadataGUID is None:
		raise Exception("root elem not found")

	crateGUIDFilter = lambda elem: elem.get('@id') == rootMetadataGUID
	rootMetadataElem = filterOneMetadataGraph(crateMetadata['@graph'], crateGUIDFilter)
	return rootMetadataElem


def getIndexGraphFilter(metadataGraph: List[Dict], queryFunc: Callable)-> int:
	""" Return the Index of an element matching the query filter

	:param metadataGraph: input metadata graph
	:type metadataGraph: List[Dict]
	:param queryFunc: filter function to 
	:type queryFunc: Callable
	:rtype: int
	"""
	indexList = [ i for i, elem in  enumerate(metadataGraph) if queryFunc(elem)]
	if len(indexList) != 1:
		raise Exception
	else:
		return indexList[0]


def pruneROCrate(metadataGraph: List, filterFunc: Callable) -> None:
	""" Filter and remove an element from the metadata graph as specified by the filter function 

	:param metadataGraph: input metadata graph
	:type metadataGraph: List[Dict]
	:param queryFunc: filter function to 
	:type queryFunc: Callable
	:rtype: Int
	"""
	elemIndex = getIndexGraphFilter(metadataGraph, filterFunc)
	metadataGraph.pop(elemIndex)


def formatROCrateToModel(inputCrate: Dict)-> ROCrate:
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
	rootElem['@graph'] = formattedCrateMetadataGraph

	# overwrite the metadata graph
	rootElem['@type'] = 'https://w3id.org/EVI#ROCrate'

	# validate that root metadata elem works for rocrate
	return ROCrate.model_validate(rootElem)