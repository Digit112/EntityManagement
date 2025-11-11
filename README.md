# EntityManagement
The EntityManager class, which corresponds to a database, contains a dictionary of RelationManager instances which each correspond to a table.
A RelationManager can be obtained via the name it was registered under, which is also the name of the table it manages:

```
# Get a RelationManager instance.
relation_mgr = entity_mgr.with_table("table_name")
```

They are created using the manage_table method:

```
# Internally calls RelationManager() and saves it.
entity_mgr.manage_table("table_name", MyEntityModel)
```

At the time it is created, a RelationManager will enumerate and cache the columns in its table.
Critically, at the moment, a RelationManager will **always** assume that a column called "id" exists in its table and is the primary key.
If this is not the case, an error will be thrown. Fuck you.

The RelationManager will also automatically update the created_on and updated_on columns if they exist, in response to CRUD operations.

Each RelationManager contains an "entity model" which is the class that was passed to `manage_table()`.
The passed entity model must inherit EntityModel, which provides context management. an EntityModel instance corresponds to a single row in the table.
An instance or instances of the model are returned by `read()` or `read_by_column()` method calls on the RelationManager.

These instances **do not** need to declare their fields.
These fields are generated automatically from the column names of the table they are generated from.
Instead, the purpose of allowing the user to supply an EntityModel is to allow them to define custom methods on the object, including the constructor which can initialize `NOT NULL` fields.

The RelationManager allows the user to retrieve a blank, "unbound" instance of its entity type. Unbound means that the primary key field is None, and therefore the entity has no corrallary in the database.
Only unbound entities can be passed to a RelationManager's `create()` method, which returns a bound copy of the item by retrieving the id after performing an insertion.
Only a bound entity can be passed to a RelationManager's `update()` method.

### Accessing Entity Data

Entity values can be accessed and modified using get_value() and set_value() or via the member access operators. The column names can be specified alone or with the table name prefixed, which in some cases is necessary to eliminate ambiguity.

```
# Equivalent
my_entity.get_value("table_name.id")
my_entity.get_value("id")
my_entity.table_name.id
my_entity.id

# Equivalent
my_entity.set_value("table_name.id", 5)
my_entity.set_value("id", 5)
my_entity.table_name.id = 5
my_entity.id = 5
```

### Entity Context Management

Entities should be used within a context manager as such:

```
with entity_mgr.with_table("accounts").read(acct_id) as my_account:
	...
```

This ensures that changes are eventually committed to the database, either via a `create()` or `update()` depending on whether the entity is bound at the time that the context manager is exited.

### Joining Tables

The Data Access Model allows the ad-hoc construction of JoinedRelationManagers, single-use derivatives of a RelationManager which represents the join between two RelationManagers. The join condition must be an equality of a column from the left table and a column from the right table. The tables may be aliased to allow self-joins or simply for convenience's sake.

```
entity_mgr.with_table("users").inner_join("projects",
	left_key="id", right_key="owner_id", left_alias="u", right_alias="p"
).read_by_column("u.id", 1)
```

In the above, note the alias-qualified "u.id" name. This could be "users.id". The left_key does not require similar qualification since it must be on the left table. If, however, the left table was itself a join, then disambiguation would again be necessary.

The entity_model of JoinedRelationManager is JoinedEntityModel - a derivitave of EntityModel, of course.  JoinedRelationManager.new_blank_enttiy() is overidden to call the constructor of its entity_model.

The EntityModel class does not have a constructor so that deriving classes need not call the super constructor. JoinedEntityModel, however, is not meant to be derived from. It's constructor receives the JoinedRelationManager that created it. This gives it access to the relations being joined and their aliases.

From then on, the JoinedEntityModel provides its user with access to its tables and columns. These values can be accessed exactly the same as on any EntityModel - using the `get_value()` and `set_value()` methods or by using the member access overloads.

## TODO

- Sort out text management with database to ensure proper handling of casing.
- Replace use of table + alias combo with AliasedTable class.
	- JoinedRelationManager.get_tables_with_qualifiers() will return this new type.
- A syntax for constructing SQL conditionals using overloaded boolean operators which can accept booleans for runtime boolean simplification.
- Switch from member-access overloads to item-access (getitem, setitem).
- Pass an array to read_by_column to use the SQL "IN" operator instead of checking equality.
