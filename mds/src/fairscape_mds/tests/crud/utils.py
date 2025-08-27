import os
import json


def load_test_data(filename):
	filepath = os.path.join(os.path.dirname(__file__), "data", filename)
	with open(filepath, 'r') as f:
		return json.load(f)
