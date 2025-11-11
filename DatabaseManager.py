from datetime import datetime
import sqlite3
from uuid import UUID

from .VoidLog import VoidLog

class ColumnInfo:
	def __init__(self, table_name, name, type, nullable, default_val, pk):
		self.table_name = table_name
		self.name = name
		self.type = type
		self.nullable = nullable
		self.default_val = default_val
		self.pk = pk
	
	# Function is a bit off, requires research... benched for now.
	# The ColumnInfo type field is literally the unaltered type specifier text from the create table statement, which can be basically anything.
	# See getting_defaults_sqlite.md for details
	# # Attempts to check if the passed value is the passed value is the column's default value.
	# def is_default(val):
		# type = self.type
		# if type(val) == "bytes":
			# val = "X'" + val.hex().upper() + "'"
			# type = type.upper()
		# else:
			# val = str(val)
		
		# return val == type

class DatabaseManager:
	def __init__(self, db_conn_str, database_log=VoidLog()):
		self.db_conn_str = db_conn_str
		self.database_log = database_log
		
		sqlite3.register_converter(
			"timestamp", lambda v: datetime.fromisoformat(v.decode())
		)
		
		sqlite3.register_adapter(
			datetime, lambda v: v.isoformat()
		)
		
		sqlite3.register_converter(
			"uuid", lambda v: UUID(bytes=v)
		)
		
		sqlite3.register_adapter(
			UUID, lambda v: v.bytes
		)
	
	def run_script(self, sql_file):
		# Connect to database and instance the schema.
		conn = self.get_connection()
		
		sql_script = sql_file.read()
		crsr = conn.cursor()
		crsr.executescript(sql_script)
		
		conn.commit()
		conn.close()
	
	# Prevents SQL injection (even though it should be impossible anyway)
	# by verifying the validity of the column/table names which are going to be spliced into a SQL statement.
	def validate_sql_identifiers(self, identifiers):
		for identifier in identifiers:
			if not isinstance(identifier, str):
				raise TypeError(f"Identifier must be string, not {type(identifier)}.")
			
			if len(identifier) == 0:
				raise TypeError(f"Identifier must not be empty string.")
			
			for character in identifier:
				if not character.isascii() or (character != "_" and not character.isalnum()):
					self.database_log.critical(f"Detected invalid SQL identifier name which could present a possible route for SQL injection: {identifier}")
					raise ValueError("Invalid SQL identifier.")
	
	def get_connection(self):
		conn = sqlite3.connect(self.db_conn_str, detect_types=sqlite3.PARSE_DECLTYPES, autocommit=False)
		conn.row_factory = sqlite3.Row
		
		conn.execute("PRAGMA foreign_keys = ON")
		
		return conn
	
	# Meant to be called only during RelationManager instantiation, not regularly!
	def columns_of(self, table_name):
		self.validate_sql_identifiers([table_name])
		
		conn = self.get_connection() # Nothing to commit.
		crsr = conn.execute("PRAGMA table_info(" + table_name + ")")
		
		ret = []
		for column in crsr:
			ret.append(ColumnInfo(table_name, column["name"], column["type"], not column["notnull"], column["dflt_value"], column["pk"]))
		
		# TODO: Perform INSERT DEFAULT VALUES and SELECT to get the defaults too.
		
		conn.close()
		return ret