from EntityManager import *

# This file defines constructs that allow the logical joining of SQL tables.
# These joins produce a tree structure of JoinedRelationManager's terminating in RelationManager leaf nodes.
# Such a RelationManager can be queried like any other.

# TODO: Implement some system to avoid duplicate aliases/names.
# TODO: Rename recurse-only parameters with a preceeding underscore.

# The EntityModel for objects stored on JOIN'd relations, corresponding to individual rows in the result of a JOIN.
# Automatically constructed as the return types of queries on JoinedRelationManaager
class JoinedEntityModel(EntityModel):
	def __init__(self, joined_relation):
		if type(joined_relation) is not JoinedRelationManager:
			raise TypeError(f"joined_relation must be JoinedRelationManager, not {joined_relation}.")
		
		self.set_relation_mgr(joined_relation)
		
		self.left_relation = joined_relation.left_relation
		self.right_relation = joined_relation.right_relation
		
		self.left_model = self.left_relation.entity_model
		self.right_model = self.right_relation.entity_model
		
		self.left_entity = self.left_relation.new_blank_entity()
		self.right_entity = self.right_relation.new_blank_entity()
	
	def get_left_relation_mgr(self):
		return self.left_entity.get_relation_mgr()
	
	def get_right_relation_mgr(self):
		return self.right_entity.get_relation_mgr()
	
	def get_left_alias(self):
		return self.get_relation_mgr().left_alias
	
	def get_right_alias(self):
		return self.get_relation_mgr().right_alias
	
	# Convert to JSON-serializable dict.
	def to_dict(self):
		res = {}
		
		left_dict = self.left_entity.to_dict()
		if type(self.left_relation) is JoinedEntityModel:
			for table in left_dict:
				res[table] = left_dict[table]
			
		else:
			table = self.get_left_alias() if self.get_left_alias() is not None else self.get_left_relation_mgr().get_table_name()
			res[table] = left_dict
		
		right_dict = self.right_entity.to_dict()
		if type(self.right_relation) is JoinedEntityModel:
			for table in right_dict:
				res[table] = right_dict[table]
			
		else:
			table = self.get_right_alias() if self.get_right_alias() is not None else self.get_right_relation_mgr().get_table_name()
			res[table] = right_dict
		
		return res
	
	def put(self, obj):
		raise RuntimeError("Cannot 'put' JoinedEntityModel.")
	
	def patch(self, obj):
		raise RuntimeError("Cannot 'patch' JoinedEntityModel.")
	
	#### Column & Table Retrieval, Modification ####
	
	# Get or set the value of the specified column.
	# Optionally, a table_name_or_alias can be provided for disambiguation.
	# Note that self_alias is not used. It must be included to match the signature of RelationManager.get_value_or_none()
	def value_accessor(self, column, self_alias, am_setting, new_value, depth):
		#print("joined value_accessor", column, self_alias, am_setting, new_value, depth)
		print("  "*depth + f"{column}{f" := {new_value}"}")
		
		if depth >= 128:
			raise RuntimeError("JOIN depth limit exceeded.")
		
		if not am_setting and new_value is not None:
			raise valueError("new_value must be None if am_setting is False.")
		
		# Retrieve results of value_accessor from sub-relations.
		left_result, right_result = (None, None)
		left_result_exists, right_result_exists = (True, True)
		
		try:
			left_result = self.left_entity.value_accessor(column, self.get_left_alias(), am_setting, new_value, depth+1)
		except (ColumnRetrievalError):
			left_result_exists = False
		
		try:
			right_result = self.right_entity.value_accessor(column, self.get_right_alias(), am_setting, new_value, depth+1)
		except (ColumnRetrievalError):
			right_result_exists = False
		
		print(f"Got {left_result} ({"exists" if left_result_exists else "not exists"}) else {right_result} ({"exists" if right_result_exists else "not exists"})")
		
		# Check for multiple results (ambiguity)
		if left_result_exists and right_result_exists:
			raise ValueError(f"Column name '{column}' is ambiguous.")
		
		if right_result_exists:
			return right_result
		
		elif left_result_exists:
			return left_result
		
		else:
			print(self.left_entity.__dict__)
			raise ColumnRetrievalError(f"Column name '{column}' does not exist.")
	
	# Gets leaf node - aka EntityModel - on the table
	def get_child_entity_model_or_none(self, table_name_or_alias, self_alias=None, depth=0):
		if depth == 0: # Validate input.
			self.get_relation_mgr().entity_mgr.db_mgr.validate_sql_identifiers([table_name_or_alias])
		
		elif depth >= 128:
			raise RuntimeError("JOIN depth limit exceeded.")
		
		left_result = self.left_entity.get_child_entity_model_or_none(table_name_or_alias, self.get_left_alias(), depth+1)
		right_result = self.right_entity.get_child_entity_model_or_none(table_name_or_alias, self.get_right_alias(), depth+1)
		print(f"Got {left_result} / {right_result}")
		
		# Check for multiple results (ambiguity)
		if left_result is not None and right_result is not None:
			raise ValueError(f"Table name or alias '{table_name_or_alias}' is ambiguous.")
		
		if left_result is None:
			return right_result # May be None
		else:
			return left_result
	
	# Attempt to access column or table values
	# def __getattr__(self, name):
		# found_table = get_child_entity_model_or_none(name)
		# if found_table is not None:
			# return found_table
		
		# found_column = get_value_or_none(name)
		# if found_table is not None:
			# return found_table
		
		# raise AttributeError(f"Identifier '{name}' does not identify any column or any table by name or alias.")
	
		
# Exposes CRUD operations on a joined table.
# The only supported join condition is equality between a column from each table.
class JoinedRelationManager(RelationManager):
	# The alias parameters allow disambigution of column names passed as the keys on this JoinedRelationManager or any that includes this one as a component, directly or indirectly.
	# These aliases appear in the generated SQL exactly as one would expect.
	def __init__(self, left_relation, right_relation, left_key, right_key, join_type=RelationManager.JoinType.INNER, left_alias=None, right_alias=None):
		print("JoinedRelationManager", left_relation, right_relation, left_key, right_key, join_type, left_alias, right_alias)
	
		if not isinstance(left_relation, RelationManager):
			raise TypeError(f"left_relation must be a RelationManager, not {type(left_relation)}")
		if not isinstance(right_relation, RelationManager):
			raise TypeError(f"right_relation must be a RelationManager, not {type(right_relation)}")

		if type(left_key) is not str:
			raise TypeError(f"left_key must be a string, not {type(left_key)}")
		if type(right_key) is not str:
			raise TypeError(f"right_key must be a string, not {type(right_key)}")

		if left_alias is not None and isinstance(left_relation, JoinedRelationManager):
			raise TypeError("left_alias must be None if left_relation is a joined relation.")
		if right_alias is not None and isinstance(right_relation, JoinedRelationManager):
			raise TypeError("right_alias must be None if left_relation is a joined relation.")

		if not isinstance(join_type, RelationManager.JoinType):
			raise TypeError(f"join_type must be a JoinType, not {type(join_type)}")
		
		if left_relation.entity_mgr is not right_relation.entity_mgr:
			raise ValueError("The constituent tables must be managed by the same EntityManager.")
		
		# TODO: Is it okay that the tables on the relations are not checked?
		
		# Throws on invalid or ambiguous column identifiers.
		# Calls validate_sql_identifiers by default, even though its real job is more just to check that the column names exist.
		left_key = left_relation.get_validated_column_identifier(ColumnIdentifier(left_key), left_alias)
		right_key = right_relation.get_validated_column_identifier(ColumnIdentifier(right_key), right_alias)

		self.left_relation = left_relation
		self.right_relation = right_relation

		self.left_key = left_key
		self.right_key = right_key

		self.join_type = join_type
		
		self.left_alias = left_alias
		self.right_alias = right_alias
		
		super().__init__(
			left_relation.entity_mgr,
			left_relation.entity_log,
			None,
			JoinedEntityModel
		)
	
	#### Internal Methods & Utilities ####
	
	# Override. Called by super constructor.
	def validate_pk_id_exists(self):
		pass
	
	# Override. Called by super constructor.
	def initialize_columns(self):
		pass
	
	# List all tables descending from this join.
	def get_all_table_names(self, depth=0):
		if depth >= 128:
			raise RuntimeError("JOIN depth limit exceeded.")
		
		return self.left_relation.get_all_table_names(depth+1) + self.right_relation.get_all_table_names(depth+1)
	
	def get_columns(self):
		return self.left_relation.get_columns() + self.right_relation.get_columns()
	
	# Returns all the columns of the descendant tables with appropriate qualifications.
	def get_column_identifiers(self):
		left_columns = self.left_relation.get_column_identifiers()
		right_columns = self.right_relation.get_column_identifiers()
		
		# Swap table name for alias if we have it.
		if self.left_alias is not None:
			for column in left_columns:
				column.qualifier = self.left_alias
		
		if self.right_alias is not None:
			for column in right_columns:
				column.qualifier = self.right_alias
		
		return list(left_columns) + list(right_columns)
	
	# Returns a SQL expression which corresponds to the relation managed by this JoinedRelationManager.
	# For the base class, this is just the name of the manged table.
	def get_validated_relation_expression(self):
		left_relation_expression = self.left_relation.get_validated_relation_expression()
		right_relation_expression = self.right_relation.get_validated_relation_expression()
		
		if self.left_alias is not None:
			left_relation_expression += f" AS {self.left_alias}"
		
		if self.right_alias is not None:
			right_relation_expression += f" AS {self.right_alias}"
		
		join_expression = f"{self.join_type.name} JOIN"
		
		return f"{left_relation_expression} {join_expression} {right_relation_expression} ON {self.left_key} = {self.right_key}"
	
	def get_table_name(self):
		raise RuntimeError("No table name on JoinedRelationManager.")

	# Checks that a column exists on this table. Throws if it doesn't, or if it is ambiguous.
	# Accepts an alias from a parent JoinedRelationManager.
	# When called from such a parent, the column_name will have already been split into a table identifier / column identifier pair.
	# This is the case when depth is positive.
	def get_validated_column_identifier(self, column, self_alias=None, depth=0):
		print("joined get_validated_column_identifier", column, self_alias, depth)
		
		if self_alias is not None:
			raise ValueError("Cannot alias a JoinedRelationManaager.")
		
		if depth == 0:
			if type(column) is not ColumnIdentifier:
				raise TypeError(f"column must be ColumnIdentifier, not {type(column)}.")
		
			if column.qualifier is not None:
				self.entity_mgr.db_mgr.validate_sql_identifiers([column.name, column.qualifier])
			else:
				self.entity_mgr.db_mgr.validate_sql_identifiers([column.name])
		
		# Retrieve column from children.
		left_result, right_result = (None, None)
		left_result_exists, right_result_exists = (True, True)
		
		print(f"aliases: {self.left_alias} / {self.right_alias}")
		
		try:
			left_result = self.left_relation.get_validated_column_identifier(column, self.left_alias, depth+1)
		except (ColumnRetrievalError):
			left_result_exists = False
		
		try:
			right_result = self.right_relation.get_validated_column_identifier(column, self.right_alias, depth+1)
		except (ColumnRetrievalError):
			right_result_exists = False
		
		if left_result_exists and right_result_exists:
			raise ColumnRetrievalError(f"Column name '{column}' is ambiguous.")
		
		# Return the one column.
		if left_result_exists:
			return left_result
		
		elif right_result_exists:
			return right_result
		
		else:
			raise ColumnRetrievalError(f"Column name '{column}' does not exist.")
	
	# Returns a blank instance of the entity that this manages
	# Such an entity is inherently suitable for CRUD operations.
	def new_blank_entity(self):
		entity = self.entity_model(self)
		# for column_name in self.get_column_names():
			# if not hasattr(entity, column_name):
				# setattr(entity, column_name, None) TODO
		
		return entity