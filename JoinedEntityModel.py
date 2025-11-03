from ColumnIdentifier import ColumnIdentifier, ColumnRetrievalError
from EntityModel import EntityModel

# The EntityModel for objects stored on JOIN'd relations, corresponding to individual rows in the result of a JOIN.
# Automatically constructed as the return types of queries on JoinedRelationManaager
class JoinedEntityModel(EntityModel):
	def __init__(self, joined_relation):
		from JoinedRelationManager import JoinedRelationManager
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