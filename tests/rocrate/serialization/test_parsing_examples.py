# import rocrate models
import os
import sys

#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
#os.path.join(os.path.dirname(__file__), '../')
srcPath = os.path.abspath('../../../src' )
sys.path.insert(0, srcPath)

import pathlib
import json
from fairscape_mds.models.rocrate import ROCrateV1_2
from unittest import TestCase

class TestROCrateParsing(TestCase):

	def test_parsing_example_crates(self):

		allCrates = [
			'data/1.cm4ai_chromatin_mda-mb-468_untreated_apmsloader_initialrun0.1alpha',
			'data/1.cm4ai_chromatin_mda-mb-468_untreated_imageloader_initialrun0.1alpha',
			'data/2.cm4ai_chromatin_mda-mb-468_untreated_apmsembed_initialrun0.1alpha',
			'data/2.cm4ai_chromatin_mda-mb-468_untreated_imageembedfold1_initialrun0.1alpha',
			'data/2.cm4ai_chromatin_mda-mb-468_untreated_imageembedfold2_initialrun0.1alpha',
			'data/3.cm4ai_chromatin_mda-mb-468_untreated_coembedfold1_initialrun0.1alpha',
			'data/3.cm4ai_chromatin_mda-mb-468_untreated_coembedfold2_initialrun0.1alpha',
			'data/4.cm4ai_chromatin_mda-mb-468_untreated_hierarchy_initialrun0.1alpha',
			'data/cm4ai_chromatin_mda-mb-468_paclitaxel_ifimage_0.1_alpha',
			'data/cm4ai_chromatin_mda-mb-468_vorinostat_ifimage_0.1_alpha',
			'data/cm4ai_chromatin_mda-mb-468_untreated_apms_0.1_alpha',
		]

		for index, crate in enumerate(allCrates):
			print(index)
			cratePath = pathlib.Path(crate) / 'ro-crate-metadata.json'

			with cratePath.open('r') as crateFile:
				crateMetadataNew = json.load(crateFile)	
				parsedMetadata = ROCrateV1_2.model_validate(crateMetadataNew)
			print(f"Success: {str(cratePath)}")