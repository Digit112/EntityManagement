from datetime import datetime, UTC
from enum import Enum

from EntityManagement.ColumnIdentifier import ColumnIdentifier, ColumnRetrievalError, ReadResultError
from EntityModel import EntityModel

# Exposes CRUD operations on a single table in the database.
# Automatically manages the created_on, updated_on, and id columns if they exist.
# Queries which return EntityModel(s) populate attributes based on columns in the database.
class RelationManager:
	class JoinType(Enum):
		INNER = 1
		OUTER = 2
		LEFT = 3
		RIGHT = 4
	
	# TODO: Validate table exists
	def __init__(self, entity_mgr, entity_log, table_name, entity_model):
		self.entity_mgr = entity_mgr
		self.entity_log = entity_log
		
		if table_name is not None: # May be none on JoinedRelationManager.
			self.entity_mgr.db_mgr.validate_sql_identifiers([table_name])
			
		self.table_name = table_name
		self.entity_model = entity_model
		
		self.columns = None
		self.initialize_columns()
		
		# All managed tables must have a column 'id' which is the primary key.
		self.validate_pk_id_exists()
	
	#### Internal Methods & Utilities ####
	
	# Overriden by JoinedRelationManager
	def validate_pk_id_exists(self):
		found_pk_id = False
		for col in self.columns:
			if col.name == "id" or col.pk:
				if col.pk and col.name == "id":
					found_pk_id = True
				
				break
		
		if not found_pk_id:
			raise ValueError(f"On '{self.get_table_name()}', all managed tables must have a column 'id' which is the primary key.")
	
	# Retrieves a list of the columns on this RelationManager.
	# Overriden by JoinedRelationManager
	def initialize_columns(self):
		if self.columns is not None:
			raise RuntimeError("Columns must be initialized only once.")
		
		self.columns = self.entity_mgr.db_mgr.columns_of(self.table_name)
	
	# Recurse call
	def get_all_table_names(self, depth=0):
		return [self.get_table_name()]
	
	def get_columns(self):
		return self.columns
	
	# Returns a SQL expression which corresponds to the relation managed by this RelationManager.
	# For the base class, this is just the name of the manged table.
	def get_validated_relation_expression(self):
		self.entity_mgr.db_mgr.validate_sql_identifiers([self.table_name])
		return self.table_name
	
	# Overloaded to throw on JoinedRelationManager
	def get_table_name(self):
		self.entity_mgr.db_mgr.validate_sql_identifiers([self.table_name])
		return self.table_name
	
	# Gets a list of column names.
	# Excludes fields which are None in the passed entity AND have no specified default in the table.
	def get_column_names_to_create(self, entity):
		res = []
		for column in self.get_columns():
			if column.default_val != None or getattr(entity, column.name) != None:
				res.append(column.name)
		
		return res
	
	# Mostly included for its semantic similarity to get_column_names_to_create
	# Returns the names of all columns on the table using this EntityManager.
	def get_column_names(self, do_lower=True):
		res = []
		for column in self.get_columns():
			res.append(column.name)
			
			if do_lower:
				res[-1] = res[-1].lower()
		
		return res
	
	# Returns a ColumnIdentifier for every column onm the managed table
	# The qualifier
	def get_column_identifiers(self):
		res = []
		for column_name in self.get_column_names():
			res.append(ColumnIdentifier(
				qualifier = self.get_table_name(),
				name = column_name
			))
		
		return res
	
	# Retrieves the underlying values of a list of attributes on an object.
	# Used in the construction of arbitrary INSERT statements.
	def get_values_of_columns(self, entity, column_names):
		res = []
		for column_name in column_names:
			res.append(getattr(entity, column_name))
		
		return res

	# Checks that a column exists on this table. Throws if it doesn't
	# Accepts an alias from a parent JoinedRelationManager.
	# When called from such a parent, the column_name will have already been split into a table identifier / column identifier pair.
	# This is the case when depth is positive.
	def get_validated_column_identifier(self, column, self_alias=None, depth=0):
		print("get_validated_column_identifier", column, self_alias, depth)
		
		# Check SQL values for invalid characters.
		if depth == 0:
			if type(column) is not ColumnIdentifier:
				raise TypeError(f"column must be ColumnIdentifier, not {type(column)}.")
		
			if column.qualifier is not None:
				self.entity_mgr.db_mgr.validate_sql_identifiers([column.name, column.qualifier])
			else:
				self.entity_mgr.db_mgr.validate_sql_identifiers([column.name])
			
			if self_alias is not None:
				self.entity_mgr.db_mgr.validate_sql_identifiers([self_alias])
		
		managed_table_name = self.get_table_name()
		
		qualifier_is_valid = True
		if column.qualifier is not None:
			if self_alias is not None:
				qualifier_is_valid = column.qualifier.lower() == managed_table_name.lower() or column.qualifier.lower() == self_alias.lower()
				print(f"  {qualifier_is_valid} := {column.qualifier.lower()} == {managed_table_name.lower()} or {column.qualifier.lower()} == {self_alias.lower()}")
			else:
				qualifier_is_valid = column.qualifier.lower() == managed_table_name.lower()
				print(f"  {qualifier_is_valid} := {column.qualifier.lower()} == {managed_table_name.lower()}")
		
		else:
			print("  No qualifier.")
		
		name_is_valid = column.name in self.get_column_names()
		
		print("  ", column.qualifier, managed_table_name, self_alias, column.name, self.get_column_names())
		
		# Validate table name and that column exists.
		if qualifier_is_valid and name_is_valid:
			column.qualifier = self_alias if self_alias else managed_table_name
			print(f"  returning {column}")
			return column
		else:
			raise ColumnRetrievalError(f"Column name '{column}' does not exist.")
	
	# Returns a blank instance of the entity that this manages
	# Such an entity is inherently suitable for CRUD operations.
	def new_blank_entity(self):
		entity = self.entity_model()
		entity.set_relation_mgr(self)
		
		for column_name in self.get_column_names():
			if column_name not in entity.__dict__:
				setattr(entity, column_name, None)
		
		return entity
	
	# Returns a new JoinedRelationManager for querying with joins.
	def join(self, right_relation, left_key, right_key, join_type=JoinType.INNER, left_alias=None, right_alias=None):
		from JoinedRelationManager import JoinedRelationManager
		
		right_relation = self.entity_mgr.with_table(right_relation)
		return JoinedRelationManager(self, right_relation, left_key, right_key, join_type, left_alias, right_alias)
	
	#### CRUD Operations ####
	
	def create(self, entity):
		if not isinstance(entity, self.entity_model):
			raise RuntimeError(f"Cannot insert '{entity}' into '{self.get_validated_relation_expression()}'.")
		
		entity.created_on = datetime.now(UTC)
		entity.updated_on = entity.created_on
		
		conn = self.entity_mgr.db_mgr.get_connection()
		crsr = conn.cursor()
		
		try:
			columns_to_create = self.get_column_names_to_create(entity)
			self.entity_mgr.db_mgr.validate_sql_identifiers(columns_to_create)
			
			values = self.get_values_of_columns(entity, columns_to_create)
			
			query_str = f"INSERT INTO {self.get_validated_relation_expression()} ({",".join(columns_to_create)}) VALUES ({",".join("?"*len(values))})"
			print(f"Executing '{query_str}', {values}")
			crsr.execute(query_str, values)
			crsr.execute("SELECT last_insert_rowid()")
			
		# TODO: Reference to sqlite3 errors couples us to this database. Offload this to the db manager class.
		except sqlite3.IntegrityError as e:
			self.entity_log.info(f"Caught IntegrityError during '{self.get_validated_relation_expression()}' creation: {e}")
			return None
		
		except sqlite3.OperationalError as e:
			self.entity_log.error(f"Caught OperationalError during '{self.get_validated_relation_expression()}' creation: {e}")
			return None
		
		else:
			entity.id = crsr.fetchone()[0] # Bind.
			entity.relation_mgr = self
			
			print("Got ID " + str(entity.id) + ", Returning")
			return entity
		
		finally:
			conn.commit()
			conn.close()
	
	def read(self, id):
		if id is None or type(id) is not int:
			raise ValueError(f"Invalid id '{id}' of type '{type(id)}'")
		
		columns_to_select = ",".join(map(lambda col : f"{repr(col)} AS [{repr(col)}]", self.get_column_identifiers()))
		
		conn = self.entity_mgr.db_mgr.get_connection()
		crsr = conn.cursor()
		query_str = f"SELECT {columns_to_select} FROM {self.get_validated_relation_expression()} WHERE id = ?"
		print(f"Executing '{query_str}' [{id}]")
		crsr.execute(query_str, (id,))
			
		entity_data = crsr.fetchone()
		conn.close()
		
		if entity_data is None:
			return None
		
		else:
			print(dict(entity_data))
			entity = self.new_blank_entity()
			for column in self.get_column_identifiers():
				entity.set_value(column, entity_data[repr(column)])
			
			print("Returning '" + str(entity) + "'")
			return entity
	
	# Returns a list of entities containing the passed matching value in the identified column.
	def read_by_column(self, column_name, matching_value):
		column = self.get_validated_column_identifier(ColumnIdentifier(column_name)) # TODO get alias here!!!!
		print(column)
		
		columns_to_select = ",".join(map(lambda col : f"{repr(col)} AS [{repr(col)}]", self.get_column_identifiers()))
		
		conn = self.entity_mgr.db_mgr.get_connection()
		crsr = conn.cursor()
		query_str = f"SELECT {columns_to_select} FROM {self.get_validated_relation_expression()} WHERE {repr(column)} = ?"
		print(f"Executing '{query_str}', {(matching_value,)}")
		crsr.execute(query_str, (matching_value,))
		
		res = []
		for entity_data in crsr:
			print(dict(entity_data))
			entity = self.new_blank_entity()
			for column in self.get_column_identifiers():
				entity.set_value(column, entity_data[repr(column)])
			
			res.append(entity)
		
		conn.close()
		return res
	
	# Reads by column, returns the entity if it exists or None otherwise.
	# Throws an error if multiple entities were found.
	def read_one_or_none_by_column(self, column_name, matching_value):
		res = self.read_by_column(column_name, matching_value)
		
		if len(res) > 1:
			raise ReadResultError(f"Expected exactly one result from read operation. Got {len(res)}.")
		
		elif len(res) == 0:
			return None
			
		else:
			return res[0]
	
	# Reads by column, returns the entity.
	# Throws an error if zero or multiple entities were found.
	def read_one_by_column(self, column_name, matching_value):
		res = self.read_by_column(column_name, matching_value)
		
		if len(res) != 1:
			raise ReadResultError(f"Expected exactly one result from read operation. Got {len(res)}.")
		
		return res[0]
	
	def update(self, entity):
		if not isinstance(entity, self.entity_model):
			raise RuntimeError(f"Cannot insert '{entity}' into '{get_validated_relation_expression()}'.")
		
		entity.updated_on = datetime.now(UTC)
		
		conn = self.entity_mgr.db_mgr.get_connection()
		crsr = conn.cursor()
		
		try:
			columns_to_update = self.get_column_names()
			columns_to_update.remove("id")
			self.entity_mgr.db_mgr.validate_sql_identifiers(columns_to_update)
			
			values = self.get_values_of_columns(entity, columns_to_update)
			values.append(entity.id)
			
			query_str = f"UPDATE {self.get_validated_relation_expression()} SET {",".join(map(lambda v : v + "=?", columns_to_update))} WHERE id = ?"
			print(f"Executing '{query_str}', {values}")
			crsr.execute(query_str, values)
			
		# TODO: Reference to sqlite3 errors couples us to this database. Offload this to the db manager class.
		except sqlite3.IntegrityError as e:
			self.entity_log.info(f"Caught IntegrityError during '{self.get_validated_relation_expression()}' creation: {e}")
			return None
		
		except sqlite3.OperationalError as e:
			self.entity_log.error(f"Caught OperationalError during '{self.get_validated_relation_expression()}' creation: {e}")
			return None
		
		else:
			entity.relation_mgr = self # Bind.
			return entity
		
		finally:
			conn.commit()
			conn.close()
	
	def delete(self, id):
		if id is None or type(id) != int:
			raise ValueError("Invalid id '" + str(id) + "' of type '" + str(type(id)) + "'")
		
		conn = self.entity_mgr.db_mgr.get_connection()
		crsr = conn.cursor()
		query_str = f"DELETE FROM {self.get_validated_relation_expression()} WHERE id = ?"
		print(f"Executing '{query_str}', [{id}]")
		crsr.execute(query_str, (id,))
		
		conn.commit()
		conn.close()
	
	#### Syntactic Sugar ####
	
	def inner_join(self, right_relation, left_key, right_key, left_alias=None, right_alias=None):
		return self.join(right_relation, left_key, right_key, RelationManager.JoinType.INNER, left_alias, right_alias)
	
	def outer_join(self, right_relation, left_key, right_key, left_alias=None, right_alias=None):
		return self.join(right_relation, left_key, right_key, RelationManager.JoinType.OUTER, left_alias, right_alias)
	
	def left_join(self, right_relation, left_key, right_key, left_alias=None, right_alias=None):
		return self.join(right_relation, left_key, right_key, RelationManager.JoinType.LEFT, left_alias, right_alias)
	
	def right_join(self, right_relation, left_key, right_key, left_alias=None, right_alias=None):
		return self.join(right_relation, left_key, right_key, RelationManager.JoinType.RIGHT, left_alias, right_alias)