from datetime import datetime, UTC
from enum import Enum
import sqlite3
import traceback

from VoidLog import VoidLog

# TODO: Rename recurse-only parameters with a preceeding underscore.

# Caught & Suppressed under certain circumstances during column retrieval.
# Mainly because we often check many joined tables for a column, expecting that all but one will throw this error.
class ColumnRetrievalError(AttributeError):
	pass

# Raised by read_one_by_column.
class ReadResultError(RuntimeError):
	pass

# Holds an identifier for a column with a qualifier (table or alias) component and a name element.
# Used to get the names in various forms.
class ColumnIdentifier:
	# Provide the qualifier and name separately, or as one string with a delimiting period.
	def __init__(self, qualified_name=None, qualifier=None, name=None):
		# Type checking
		if qualifier is not None and type(qualifier) is not str:
			raise TypeError(f"qualifier must be string, not {type(qualifier)}.")
		
		if name is not None and type(name) is not str:
			raise TypeError(f"name must be string, not {type(name)}.")
		
		if qualified_name is not None and type(qualified_name) is not str:
			raise TypeError(f"qualified_name must be string, not {type(qualified_name)}.")
		
		# Split qualified name into components.
		if qualified_name is not None:
			if name is not None or qualifier is not None:
				raise valueError("name and qualifier must be None if qualified_name is not None.")
			
			comps = qualified_name.split(".", 1)
			if len(comps) == 1:
				qualifier = None
				name = comps[0]
			
			elif len(comps) == 2:
				qualifier = comps[0]
				name = comps[1]
			
			else:
				raise ValueError(f"qualified_name '{qualified_name}' is not valid.")
		
		self.qualifier = qualifier
		self.name = name
	
	def __eq__(self, other):
		if self.qualifier is None != other.qualifier is None:
			raise RuntimeError("Invalid equality check between qualified and unqualified ColumnIdentifier.")
		
		return self.qualifier == other.qualifier and self.name == other.name
	
	def __repr__(self):
		return f"{self.qualifier}.{self.name}"
	
	def __str__(self):
		return f"[{self.qualifier}].[{self.name}]"

# Holds canonical copies of managers for the various entities in the database.
class EntityManager:
	def __init__(self, db_mgr, entity_log):
		self.db_mgr = db_mgr
		self.entity_log = entity_log
		
		self.tables = {}
	
	def manage_table(self, table_name, entity_model):
		if type(table_name) is not str:
			raise TypeError(f"table_name must be string, not {type(table_name)}.")
		
		if type(entity_model) is not type or not issubclass(entity_model, EntityModel):
			raise TypeError(f"entity_model must be a class which inherits EntityModel, not {entity_model}.")
		
		self.tables[table_name] = RelationManager(self, self.entity_log, table_name, entity_model)
	
	# Acquires the named table manager which can be used to perform CRUD operations on a specific kind of item.
	def with_table(self, table_name):
		if table_name in self.tables:
			return self.tables[table_name]
		else:
			raise RuntimeError("Invalid Table '" + table_name + "'")

# Base class for objects stored by the database, corresponding to individual rows in tables.
# Deriving classes will be returned by queries on their associated table which return data.
# Deriving classes need not list the columns on a table as these are retrieved by the RelationManager for that table.
# Deriving classes have no obligations as to the overloads they must provide.
class EntityModel:
	# No constructor, so that deriving classes do not need to use the super constructor.
	
	def set_relation_mgr(self, relation_mgr):
		object.__setattr__(self, "relation_mgr", relation_mgr)
	
	def get_relation_mgr(self):
		# Detailed error checking for initialization, since the constructor is not defined.
		try:
			return self.__dict__["relation_mgr"]
			
		except (KeyError):
			raise RuntimeError("relation_mgr was not initialized.")
	
	# Context management
	def __enter__(self):
		return self
	
	def __exit__(self, exc_type, exc_value, exc_tb):
		if exc_value is not None:
			raise exc_value
		
		if self.id is None:
			self.get_relation_mgr().create(self)
		else:
			self.get_relation_mgr().update(self)
	
	# Convert to JSON-serializable dict.
	# Allows specification of include_columns_as, a dict for mapping existing columns onto new names.
	def to_dict(self, include_columns_as={}):
		res = {}
		for column_name in self.get_relation_mgr().get_column_names():
			if column_name in include_columns_as:
				res[include_columns_as[column_name]] = self.get_value(column_name)
			else:
				res[column_name] = self.get_value(column_name)
		
		return res
	
	# Update all columns from a dict.
	def put(self, obj):
		for column_name in self.get_relation_mgr().get_column_names():
			if column_name not in obj:
				raise TypeError(f"Cannot put entity which is missing column name '{column_name}'.")
			
			self.set_value(column_name, obj[column_name])
	
	# Update some columns from a dict.
	def patch(self, obj):
		for column_name in self.get_relation_mgr().get_column_names():
			if column_name not in obj:
				continue
			
			self.set_value(column_name, obj[column_name])
	
	#### Column & Table Retrieval, Modification ####
	
	# Returns self if the provided name or alias matches this table.
	# The same as get_child_entity_model_or_none, except that it throws ValueError instead of returning None when the requested table does not exist.
	def get_child_entity_model(self, table_name_or_alias, self_alias=None):
		val = self.get_child_entity_model_or_none(table_name_or_alias, self_alias)
		if val is None:
			raise ValueError(f"Table name or alias '{table_name_or_alias}' does not exist.")
		
		return val
	
	# Returns self if the provided name or alias matches this table. Returns None otherwise.
	# This recursive function's signature must match JoinedRelationManager.get_child_entity_model_or_none(), the caller!
	# The alias comes from the JoinedRelationManager managing the JoinedEntityModel which this EntityModel is a direct child of.
	# That alias was passed to the JoinedRelationManager's constructor. This mechanism allows different aliases to the same table during self-joins.
	def get_child_entity_model_or_none(self, table_name_or_alias, self_alias=None, depth=0):
		if depth == 0: # Validate input.
			self.get_relation_mgr().entity_mgr.db_mgr.validate_sql_identifiers([table_name_or_alias])
		
		if table_name_or_alias.lower() == self_alias.lower() or table_name_or_alias.lower() == self.get_relation_mgr().get_validated_relation_expression().lower():
			return self
		
		else:
			return None
	
	# Gets a column's value.
	# Throws ColumnRetrievalError if the requested column does not exist.
	# Accetps a ColumnIdentifier or string. String can optionally include a qualifier.
	def get_value(self, column, self_alias=None):
		if type(column) is str:
			column = ColumnIdentifier(column)
		
		#print("get_value", column, self_alias)
		return self.value_accessor(column, self_alias, False, None, 0)
	
	# Sets the value of the specified column.
	# If provided, throws if table_name_or_alias does not match this table's name or the provided self_alias.
	def set_value(self, column, new_value=None, self_alias=None):
		if type(column) is str:
			column = ColumnIdentifier(column)
		
		#print("set_value", column, new_value, self_alias)
		self.value_accessor(column, self_alias, True, new_value, 0)
	
	def __getattr__(self, name):
		try:
			return self.get_value(name)
		except (ColumnRetrievalError):
			pass
		
		val = self.get_child_entity_model_or_none(name)
		if val is None:
			raise AttributeError(f"No such member, table, or column '{name}', or it is ambiguous.")
		
		return val
	
	# def __setattr__(self, name, new_value):
		# self.set_value(new_value, name)
	
	# This recursive function's signature must match JoinedRelationManager.value_accessor(), the caller!
	# Exposes get/set functionality on the columns of this entity.
	# The alias comes from the JoinedRelationManager managing the JoinedEntityModel which this EntityModel is a direct child of.
	# That alias was passed to the JoinedRelationManager's constructor. This mechanism allows different aliases to the self table during self-joins.
	def value_accessor(self, column, self_alias, am_setting, new_value, depth):
		#print("value_accessor", column, self_alias, am_setting, new_value, depth)
		print("  "*depth + f"[{self_alias if self_alias is not None else self.get_relation_mgr().get_table_name()}] {column}{f" := {new_value}"}")
		
		table_name_matches_self_alias = column.qualifier is not None and self_alias is not None and column.qualifier.lower() == self_alias.lower()
		table_name_matches_self_name = column.qualifier is not None and column.qualifier.lower() == self.get_relation_mgr().get_validated_relation_expression().lower()
		
		# if column.qualifier is not None:
			# print(f"{column.qualifier} against... {self_alias}, {self.get_relation_mgr().get_validated_relation_expression()}")
		
		# Validate identified table, if any.
		if column.qualifier is None or table_name_matches_self_alias or table_name_matches_self_name:
			# Validate column presence.
			if column.name.lower() in self.get_relation_mgr().get_column_names():
				if am_setting:
					return object.__setattr__(self, column.name, new_value)
				else:
					# print(self.__dict__)
					# Attribute should've been created by new_blank_entity()
					return self.__dict__[column.name]
				
			else:
				raise ColumnRetrievalError(f"Column name '{column}' does not exist.")
		
		else:
			raise ColumnRetrievalError(f"'{column.qualifier}' does not refer to this entity.")
		
		assert False

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