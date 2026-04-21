from fairscape_mds.core.config import FairscapeConfig
from fairscape_graph_tools.pipeline.graph_utils import flexible_ark_query

__all__ = ["FairscapeRequest", "flexible_ark_query"]


class FairscapeRequest():
	def __init__(
			self,
			backendConfig: FairscapeConfig
	):
		self.config = backendConfig

	def getMetadata(self, guid: str):
		return self.config.identifierCollection.find_one({"@id": guid}, projection={"_id": False})

	def flexibleFind(self, guid: str, projection=None):
		"""Look up an identifier by exact match first, then fall back to
		a dash-insensitive and ark:/ark: tolerant regex search."""
		if projection is None:
			projection = {"_id": False}
		# Exact match
		result = self.config.identifierCollection.find_one(
			{"@id": guid}, projection=projection
		)
		if result:
			return result
		# Flexible fallback
		query = flexible_ark_query(guid)
		if query:
			result = self.config.identifierCollection.find_one(
				query, projection=projection
			)
		return result

