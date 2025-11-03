from ColumnIdentifier import ColumnIdentifier, ColumnRetrievalError

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