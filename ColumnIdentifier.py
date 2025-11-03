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