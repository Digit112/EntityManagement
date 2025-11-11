from datetime import datetime
import uuid

def test_datetime_conversion(dummy_structured_database_mgr):
	db_mgr = dummy_structured_database_mgr
	
	conn = db_mgr.get_connection()
	crsr = conn.cursor()
	
	date_value = datetime(2001, 2, 2, 2, 22, 22)
	
	crsr.execute("INSERT INTO stuff (date_of) VALUES (?)", (date_value,))
	
	id = crsr.lastrowid
	crsr.execute("SELECT date_of FROM stuff WHERE id = ?", (id,))
	
	entity_data = crsr.fetchone()
	
	assert entity_data is not None
	assert "date_of" in entity_data.keys()
	assert entity_data["date_of"] == date_value
	
	conn.commit()
	conn.close()

def test_uuid_conversion(dummy_structured_database_mgr):
	db_mgr = dummy_structured_database_mgr
	
	conn = db_mgr.get_connection()
	crsr = conn.cursor()
	
	uuid_value = uuid.uuid4()
	
	crsr.execute("INSERT INTO stuff (uuid_of) VALUES (?)", (uuid_value,))
	
	id = crsr.lastrowid
	crsr.execute("SELECT uuid_of FROM stuff WHERE id = ?", (id,))
	
	entity_data = crsr.fetchone()
	
	assert entity_data is not None
	assert "uuid_of" in entity_data.keys()
	assert entity_data["uuid_of"] == uuid_value
	
	conn.commit()
	conn.close()