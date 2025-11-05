import pytest

def test_identifier_validation(db_mgr):
	db_mgr.validate_sql_identifiers("_1aAzZ_0")
	db_mgr.validate_sql_identifiers("mnop_5678")
	
	with pytest.raises(ValueError) as excinfo:
		db_mgr.validate_sql_identifiers("abc$")
	
	with pytest.raises(ValueError) as excinfo:
		db_mgr.validate_sql_identifiers("_ _")
	
	with pytest.raises(ValueError) as excinfo:
		db_mgr.validate_sql_identifiers("abc$")
	
	with pytest.raises(ValueError) as excinfo:
		db_mgr.validate_sql_identifiers("\"")

def test_create_read_1(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	new_user = entity_mgr.with_table("users").new_blank_entity()
	new_user.username = "ekobadd"
	new_user.password = "password123"
	
	entity_mgr.with_table("users").create(new_user)
	print(new_user.id)
	
	read_user = entity_mgr.with_table("users").read(new_user.id)
	assert read_user.username == "ekobadd"
	assert read_user.password == "password123"

def test_create_read_2(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	# Make user and new project they own
	new_user = entity_mgr.with_table("users").new_blank_entity()
	new_user.username = "ekobadd"
	new_user.password = "password123"
	entity_mgr.with_table("users").create(new_user)
	
	new_project = entity_mgr.with_table("projects").new_blank_entity()
	new_project.title = "Scandalines"
	new_project.owner_id = new_user.id
	entity_mgr.with_table("projects").create(new_project)
	
	# Read owner of project.
	read_project = entity_mgr.with_table("projects").read(new_project.id)
	read_user = entity_mgr.with_table("users").read(read_project.owner_id)
	
	assert read_user.username == "ekobadd"
	assert read_user.password == "password123"
	

def test_read_by_column(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	new_user = entity_mgr.with_table("users").new_blank_entity()
	new_user.username = "bipnboop"
	new_user.password = "passalasso"
	
	entity_mgr.with_table("users").create(new_user)
	print("Created ID: " + str(new_user.id))
	
	read_users = entity_mgr.with_table("users").read_one_by_column("username", "bipnboop")
	
	assert read_users.username == "bipnboop"
	assert read_users.password == "passalasso"

def test_update(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	read_user = entity_mgr.with_table("users").read_one_by_column("username", "ekobadd")
	
	read_user.set_value("username", "not ekobadd")
	read_user.set_value("password", "not password123")
	
	entity_mgr.with_table("users").update(read_user)
	print("Updated ID: " + str(read_user.id))
	
	new_read_user = entity_mgr.with_table("users").read(read_user.id)
	
	assert new_read_user.username == "not ekobadd"
	assert new_read_user.password == "not password123"

def test_entity_context_manager(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	# Test create
	with entity_mgr.with_table("users").new_blank_entity() as new_user:
		new_user.username = "created_with_context_manager"
		new_user.password = "password123"
	
	# Test update
	with entity_mgr.with_table("users").read_one_by_column("username", "created_with_context_manager") as read_user:
		assert read_user.password == "password123"
		read_user.password = "password456"
	
	read_user = entity_mgr.with_table("users").read_one_by_column("username", "created_with_context_manager")
	assert read_user.password == "password456"

def test_create_delete(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	new_user = entity_mgr.with_table("users").new_blank_entity()
	new_user.username = "im_gone_soon"
	new_user.password = "password123"
	
	entity_mgr.with_table("users").create(new_user)
	entity_mgr.with_table("users").delete(new_user.id)
	
	read_user = entity_mgr.with_table("users").read(new_user.id)
	assert read_user is None

def test_delete(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	read_user = entity_mgr.with_table("users").read_one_by_column("username", "ekobadd")
	
	with pytest.raises(TypeError):
		entity_mgr.with_table("users").delete("incorrect type")
	
	entity_mgr.with_table("users").delete(read_user.id)
	
	new_read_user = entity_mgr.with_table("users").read_by_column("username", "ekobadd")
	assert new_read_user == []
	
	new_read_user = entity_mgr.with_table("users").read(read_user.id)
	assert new_read_user is None

def create_joined(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	new_user = entity_mgr.with_table("users").new_blank_entity()
	new_user.username = "ekobadd"
	new_user.password = "password123"
	
	new_project = entity_mgr.with_table("projects").new_blank_entity()
	new_project.title = "New Project"
	new_project.owner_id = new_user.id # User owns project

def test_read_joined_and_use_all_column_access_modes(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	read_project_user = entity_mgr.with_table("users").inner_join("projects",
		left_key="id", right_key="owner_id", left_alias="u", right_alias="p"
	).read_one_by_column("username", "ekobadd")
	
	print("Setup complete...")
	
	# Test all modes of access:
	# - Unqualified function call (when column names are not ambiguous)
	# - Table-qualified function call
	# - Alias-qualified function call
	# - Unqualified member access (when column names are not ambiguous)
	# - Table-qualified member access
	# - Alias-qualified member access
	assert read_project_user.get_value("username") == "ekobadd"
	assert read_project_user.get_value("password") == "password123"
	assert read_project_user.get_value("owner_id") == read_project_user.get_value("u.id")
	assert read_project_user.get_value("title") == "ekobadds project"
	
	assert read_project_user.get_value("users.username") == "ekobadd"
	assert read_project_user.get_value("users.password") == "password123"
	assert read_project_user.get_value("projects.owner_id") == read_project_user.get_value("users.id")
	assert read_project_user.get_value("projects.title") == "ekobadds project"
	
	assert read_project_user.get_value("u.username") == "ekobadd"
	assert read_project_user.get_value("u.password") == "password123"
	assert read_project_user.get_value("p.owner_id") == read_project_user.get_value("u.id")
	assert read_project_user.get_value("p.title") == "ekobadds project"
	
	assert read_project_user.username == "ekobadd"
	assert read_project_user.password == "password123"
	assert read_project_user.owner_id == read_project_user.u.id
	assert read_project_user.title == "ekobadds project"
	
	assert read_project_user.users.username == "ekobadd"
	assert read_project_user.users.password == "password123"
	assert read_project_user.projects.owner_id == read_project_user.users.id
	assert read_project_user.projects.title == "ekobadds project"
	
	assert read_project_user.u.username == "ekobadd"
	assert read_project_user.u.password == "password123"
	assert read_project_user.p.owner_id == read_project_user.u.id
	assert read_project_user.p.title == "ekobadds project"

def test_simple_self_join(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	# Create a project and owner.
	new_user = entity_mgr.with_table("users").new_blank_entity()
	new_user.username = "ekobadd"
	new_user.password = "password123"
	
	entity_mgr.with_table("users").create(new_user)
	
	user_user = entity_mgr.with_table("users").inner_join("users",
		left_key="id", right_key="id", left_alias="u1", right_alias="u2"
	).read_one_by_column("u1.id", new_user.id)
	
	assert user_user.u1.username == "ekobadd"
	assert user_user.u1.password == "password123"
	assert user_user.u2.username == "ekobadd"
	assert user_user.u2.password == "password123"

def test_double_self_join(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	# Create three users.
	user1 = entity_mgr.with_table("users").new_blank_entity()
	user1.username = "ekobadd"
	entity_mgr.with_table("users").create(user1)
	
	user2 = entity_mgr.with_table("users").new_blank_entity()
	user2.username = "ekohboy"
	user2.manager_id = user1.id # user1 manages user2
	entity_mgr.with_table("users").create(user2)
	
	user3 = entity_mgr.with_table("users").new_blank_entity()
	user3.username = "ekono"
	user3.manager_id = user2.id # user2 manages user3
	entity_mgr.with_table("users").create(user3)
	
	# TODO: Removing the u2 from left_key does not cause an error even though the result is ambiguous!
	# Find ekono's manager and manager's manager.
	users_users_users = entity_mgr.with_table("users"
		).inner_join("users", left_key="id", right_key="manager_id", left_alias="u1", right_alias="u2"
		).inner_join("users", left_key="u2.id", right_key="manager_id", right_alias="u3"
	).read_one_by_column("u3.id", user3.id)
	
	assert users_users_users.u3.username == "ekono"
	assert users_users_users.u2.username == "ekohboy"
	assert users_users_users.u1.username == "ekobadd"

def test_double_join(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	# Create member
	new_user = entity_mgr.with_table("users").new_blank_entity()
	new_user.username = "member"
	new_user.password = "member123"
	
	entity_mgr.with_table("users").create(new_user)
	
	# Create a project
	new_project = entity_mgr.with_table("projects").new_blank_entity()
	new_project.title = "New Project"
	
	entity_mgr.with_table("projects").create(new_project)
	
	# User is member of project
	new_project_member = entity_mgr.with_table("project_members").new_blank_entity()
	new_project_member.user_id = new_user.id
	new_project_member.project_id = new_project.id
	
	entity_mgr.with_table("project_members").create(new_project_member)
	
	users_project_member = entity_mgr.with_table("users"
		).inner_join("project_members", left_key="id", right_key="user_id", left_alias="u"
		).inner_join("projects", left_key="project_id", right_key="id"
	).read_one_by_column("u.id", new_user.id)
	
	assert users_project_member.username == "member"
	assert users_project_member.user_id == new_user.id
	assert users_project_member.project_id == new_project.id
	assert users_project_member.title == "New Project"

def test_no_alias_joined_relation(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	# Must not provide an alias to a JoinedRelationManager
	with pytest.raises(TypeError):
		users_project_member = entity_mgr.with_table("users"
			).inner_join("project_members", left_key="id", right_key="user_id"
			).inner_join("projects", left_key="project_id", right_key="id", left_alias = "uhoh"
		)

def test_to_dict(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	read_user = entity_mgr.with_table("users").read_one_by_column("username", "ekobadd")
	
	dict_user = read_user.to_dict()
	
	# Check dict results match basic expectations
	assert dict_user["username"] == "ekobadd"
	
	# Check that dict contents exactly match returned entity
	for key in dict_user:
		print(key)
		assert read_user.get_value(key) == dict_user[key]

def test_joined_to_dict(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	read_project_user = entity_mgr.with_table("users").inner_join("projects",
		left_key="id", right_key="owner_id"
	).read_one_by_column("username", "ekobadd")
	
	dict_project_user = read_project_user.to_dict()
	
	# Check dict results match basic expectations
	assert dict_project_user["users"]["username"] == "ekobadd"
	assert dict_project_user["projects"]["title"] == "ekobadds project"
	
	# Check that dict contents exactly match returned entity
	for tbl_key in dict_project_user:
		print(tbl_key)
		for col_key in dict_project_user[tbl_key]:
			print(col_key)
			assert read_project_user.get_value(f"{tbl_key}.{col_key}") == dict_project_user[tbl_key][col_key]

def test_aliased_joined_to_dict(dummy_structured_entity_mgr):
	entity_mgr = dummy_structured_entity_mgr
	
	read_project_user = entity_mgr.with_table("users").inner_join("projects",
		left_key="id", right_key="owner_id", left_alias="u", right_alias="p"
	).read_one_by_column("username", "ekobadd")
	
	dict_project_user = read_project_user.to_dict()
	
	# Check dict results match basic expectations
	assert dict_project_user["u"]["username"] == "ekobadd"
	assert dict_project_user["p"]["title"] == "ekobadds project"
	
	# Check that dict contents exactly match returned entity
	for tbl_key in dict_project_user:
		print(tbl_key)
		for col_key in dict_project_user[tbl_key]:
			print(col_key)
			assert read_project_user.get_value(f"{tbl_key}.{col_key}") == dict_project_user[tbl_key][col_key]
	

# TODO:
# - Test errors thrown when JOIN depth is exceeded.
# - Test effictiveness with JoinedRelationManager constructor with different inputs on left and right.
# - Test that dictionaries obtained via to_dict can be passed to put() and patch() and work as expected.