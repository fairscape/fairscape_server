import sys, os

# import source 
sys.path.insert(
	0, 
	os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src'))
	)

from fairscape_mds.models.dataset import (
	DatasetCreateModel
)
from fairscape_models.dataset import Dataset

class TestDataset:

	def test_model_instance(self):

		# create a instance of fairscape_models.Dataset
		datasetModelInstance = Dataset.model_validate(
			{
				"@id": "ark:59852/test-guid",
				"name": "test dataset",
				"@type": "https://w3id.org/EVI#Dataset",
				"author": "John Doe",
				"datePublished": "04-08-2025",
				"version": "0.1.0",
				"file": "csv",
				"description": "An example dataset",
				"keywords": [],
				"format": "csv",
				"dataSchema": None,
				"generatedBy": [],
				"derivedFrom": [],
				"usedByComputation": [],
				"contentUrl": "https://example.org/"
			}
		)

		

		datasetCreateModel = DatasetWriteModel.model_validate()

		# convert into fairscape_mds.models.DatasetCreateModel

		# distribution with minio

		# distribution with url distribution
		assert True == True
		pass


	def test_model_publish(self):
		assert True == True
		pass

	def test_model_get(self):
		assert True == True
		pass

	def test_model_update(self):
		assert True == True
		pass