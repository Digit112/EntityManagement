
import traceback

from .VoidLog import VoidLog
from .RelationManager import RelationManager

# TODO: Rename recurse-only parameters with a preceeding underscore.

# Holds canonical copies of managers for the various entities in the database.
class EntityManager:
	def __init__(self, db_mgr, entity_log):
		self.db_mgr = db_mgr
		self.entity_log = entity_log
		
		self.tables = {}
	
	def manage_table(self, table_name, entity_model):
		if type(table_name) is not str:
			raise TypeError(f"table_name must be string, not {type(table_name)}.")
		
		from .EntityModel import EntityModel
		if type(entity_model) is not type:
			raise TypeError(f"entity_model must be a type, not {type(entity_model)}.")
		# if not issubclass(entity_model, EntityModel):
			# raise TypeError(f"entity_model must be a class which inherits EntityModel, not {entity_model}.")
		
		self.tables[table_name] = RelationManager(self, self.entity_log, table_name, entity_model)
	
	# Acquires the named table manager which can be used to perform CRUD operations on a specific kind of item.
	def with_table(self, table_name):
		if table_name in self.tables:
			return self.tables[table_name]
		else:
			raise RuntimeError("Invalid Table '" + table_name + "'")