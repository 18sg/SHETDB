# coding: utf-8
import pymongo
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from shet.client import ShetClient, shet_action
from twisted.internet import defer
from twisted.internet.threads import deferToThread
from json_util import to_json, to_bson
from functools import partial


COLLECTION_ACTIONS = "insert find find_one remove update save rename drop count".split()
CURSOR_ACTIONS = "count distinct limit skip sort where".split()
TYPES = [(Collection, COLLECTION_ACTIONS), (Cursor, CURSOR_ACTIONS)]


class ShetDb(ShetClient):
	
	def __init__(self, db, root):
		self.db = db
		self.root = root
		ShetClient.__init__(self)
		
		# Add a SHET action that calls the corresponding mongo function.
		def add_mongo_action(f_name):
			self.add_action(f_name, 
				lambda collection, *args:
					deferToThread(
						getattr(self.db.SHET[collection], f_name),
						*map(to_bson, args)
					).addCallback(to_json)
			)
		
		for f_name in COLLECTION_ACTIONS:
			add_mongo_action(f_name)
	
	@shet_action
	@defer.inlineCallbacks
	def query(self, collection, *parts):
		"""Perform a generic query, with chaining of parts into a longer query.
		For example, self.query("test", ["find", {"foo": "bar"}], "count") is
		equivalent to self.db.SHET.find({"foo": "bar"}).count()
		"""
		# Start with the named collection.
		current = self.db.SHET[collection]
		
		for part in parts:
			# Each part is in the form [name, *args], or just name.
			name, args = (part, []) if isinstance(part, basestring) else (part[0], part[1:])
			
			# Check if name is applicable to the current type.
			if any(isinstance(current, type) and name in actions for (type, actions) in TYPES):
				current = yield deferToThread(getattr(current, name), *map(to_bson, args))
			else:
				raise TypeError("Cannot apply '%s' to '%s'." % name, current)
		
		defer.returnValue(to_json(current))


if __name__ == "__main__":
	db = pymongo.Connection()
	ShetDb(db, "/mongo/").run()
