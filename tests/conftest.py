import os
import pytest

from ..DatabaseManager import DatabaseManager
from ..EntityManager import EntityManager
from ..EntityModel import EntityModel

class fake_logger:
	def __init__(self, name):
		self.name = name
	
	def debug(self, msg):
		print(f"({self.name}) debug: {msg}")
	
	def info(self, msg):
		print(f"({self.name}) info: {msg}")
	
	def warning(self, msg):
		print(f"({self.name}) warning: {msg}")
	
	def error(self, msg):
		print(f"({self.name}) error: {msg}")
	
	def critical(self, msg):
		print(f"({self.name}) critical: {msg}")

class FakeConfigManager:
	def setup_logger(self, name, level=None, fmt_str=None):
		return fake_logger(name)

@pytest.fixture()
def config_mgr(tmpdir):
	return FakeConfigManager()

@pytest.fixture()
def db_mgr(tmpdir, config_mgr):
	return DatabaseManager(tmpdir + "test_entity_management.db",
		database_log=config_mgr.setup_logger("database", "debug")
	)

@pytest.fixture()
def dummy_structured_entity_mgr(config_mgr, db_mgr):
	class User(EntityModel):
		pass
	
	class Project(EntityModel):
		pass
	
	class ProjectUser(EntityModel):
		pass
	
	# Initialize some crap in the table.
	conn = db_mgr.get_connection()
	crsr = conn.cursor()
	
	crsr.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, created_on TIMESTAMP, updated_on TIMESTAMP, username VARCHAR, password VARCHAR, manager_id INTEGER)")
	crsr.execute("CREATE TABLE projects (id INTEGER PRIMARY KEY AUTOINCREMENT, created_on TIMESTAMP, updated_on TIMESTAMP, title VARCHAR(64), owner_id INTEGER)")
	crsr.execute("CREATE TABLE project_members (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, project_id)")
	
	# Managed users for self-joins.
	crsr.execute("INSERT INTO users (username, password) VALUES ('big boss', 'bigboss123')")
	bigboss_id = crsr.lastrowid
	
	crsr.execute(f"INSERT INTO users (username, password, manager_id) VALUES ('lil boss', 'lilboss123', {bigboss_id})")
	lilboss_id = crsr.lastrowid
	
	crsr.execute(f"INSERT INTO users (username, password, manager_id) VALUES ('wagie :(', 'wagie123', {lilboss_id})")
	
	# Project with owner and member.
	crsr.execute("INSERT INTO users (username, password) VALUES ('ekobadd', 'password123')")
	owner_id = crsr.lastrowid
	
	crsr.execute(f"INSERT INTO projects (title, owner_id) VALUES ('ekobadds project', {owner_id})")
	project_id = crsr.lastrowid
	
	crsr.execute("INSERT INTO users (username, password) VALUES ('ekofren', 'password345')")
	member_id = crsr.lastrowid
	
	crsr.execute(f"INSERT INTO project_members (user_id, project_id) VALUES ({member_id}, {project_id})")
	
	conn.commit()
	conn.close()
	
	new_entity_mgr = EntityManager(db_mgr, entity_log=config_mgr.setup_logger("entity", "debug"))
	new_entity_mgr.manage_table("users", User)
	new_entity_mgr.manage_table("projects", Project)
	new_entity_mgr.manage_table("project_members", ProjectUser)
	
	return new_entity_mgr