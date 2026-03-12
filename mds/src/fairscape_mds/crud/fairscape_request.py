import re
from fairscape_mds.core.config import FairscapeConfig


def flexible_ark_query(guid: str):
	"""Build a MongoDB query that matches an ARK with or without dashes
	and with or without a slash after 'ark:'. Returns None if guid
	doesn't look like an ARK. Matches ARK SPEC"""
	ark_match = re.match(r'^ark:/?([\d]+)/(.*)', guid)
	if not ark_match:
		return None
	naan = ark_match.group(1)
	postfix = ark_match.group(2)
	stripped = postfix.replace('-', '')
	fuzzy_postfix = '-?'.join(re.escape(c) for c in stripped)
	pattern = f'^ark:{naan}/{fuzzy_postfix}$'
	return {"@id": {"$regex": pattern}}


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

