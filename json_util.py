from bson.json_util import default, object_hook
from pymongo.cursor import Cursor


def to_json(data):
	"""Convert a BSON value to something JSON serialiseable, 
	using json_util.default.
	"""
	if isinstance(data, dict):
		return dict((key, to_json(value)) for key, value in data.iteritems())
	elif isinstance(data, (list, Cursor)):
		return map(to_json, data)
	else:
		try:
			return default(data)
		except TypeError:
			return data

def to_bson(data):
	"""Convert a JSON-deserialised value to a BSON value, 
	using json_util.object_hook.
	"""
	if isinstance(data, dict):
		return object_hook(dict((key, to_bson(value)) for key, value in data.iteritems()))
	elif isinstance(data, list):
		return map(to_bson, data)
	else:
		return data
