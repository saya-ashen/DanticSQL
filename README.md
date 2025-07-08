# DanticSQL

DanticSQL is a powerful Python utility designed to efficiently reconstruct nested `SQLModel` object graphs from a flat Pandas DataFrame. It's the perfect tool for when you perform a complex SQL `JOIN` query and need to transform the resulting tabular data back into your structured, relational Pydantic/SQLModel objects.

It bridges the gap between database query results and a clean, object-oriented data representation in your application.


-----

## The Problem It Solves

When using an ORM like `SQLAlchemy` with `SQLModel`, you define neat relationships between your data models (e.g., a `User` has many `Posts`). However, when you fetch data with a `JOIN` to get all information in one query, you get a flat, denormalized table.

For example, a `LEFT JOIN` between `users` and `posts` might produce a result like this:

| user\_id | name | post\_id | title |
|---|---|---|---|
| 1 | Alice | 101 | Hello World |
| 1 | Alice | 102 | My Second Post |
| 2 | Bob | 103 | A Post by Bob |

The challenge is to efficiently parse this `DataFrame` back into two `User` objects and three `Post` objects, with `user_alice.posts` correctly containing a list of her two posts. Manually looping through this data is inefficient and error-prone. **DanticSQL automates this entire process.**

-----

## How It Works

The library operates in a two-stage process:

### 1\. Parsing and Grouping (`pydantic_all`)

First, `DanticSQL` processes the input `DataFrame` for each of your `SQLModel` tables.

  * **Grouping:** It groups the `DataFrame` by each table's primary key. This consolidates all rows belonging to a single object instance (like the two rows for `user_id=1` above).
  * **Intelligent Aggregation:**
      * For regular data fields (`name`), it takes the first value (as it should be the same across all grouped rows).
      * For fields that represent relationship keys (`post_id`), it aggregates all unique values into a `set`. This correctly captures one-to-many and many-to-many relationships.
  * **Pydantic Validation:** It uses `pydantic.TypeAdapter` for highly efficient, bulk validation and creation of your `SQLModel` instances from the cleaned data.

At the end of this stage, you have distinct, de-duplicated instances of each model, but they are not yet linked together.

### 2\. Connecting Relationships (`connect_all`)

This is where the magic happens. The `connect_all` method links the newly created objects.

  * **Lookup Maps:** It first creates fast lookup dictionaries (hash maps) for all created instances, mapping each object's primary key to the object itself (e.g., `{1: <User object with id=1>}`).
  * **Relationship Data:** It then iterates over the intermediate relationship data generated in the first step.
  * **Linking:** For each source object, it finds the corresponding foreign key values it needs to link. It uses these keys to instantly retrieve the related objects from the lookup maps and sets the relationship attribute (`user.posts` or `post.user`) accordingly.

This approach avoids slow, nested loops and is significantly more performant, especially for large datasets.

-----

## Key Features

  * **Efficient:** Leverages `pandas` for high-performance grouping and `pydantic` for fast, C-optimized data validation.
  * **Relationship Aware:** Automatically inspects `SQLModel` relationships (`One-to-Many`, `Many-to-One`, `Many-to-Many`) to guide the reconstruction process.
  * **Decoupled:** Separates the data-fetching logic (your SQL query) from the object-modeling logic.
  * **Robust:** Handles single and composite primary keys.

-----

## Installation
This package is not published on PyPI yet. You can install it directly from the GitHub repository:

```bash
pip install https://github.com/saya-ashen/DanticSQL/releases/download/0.1.0/danticsql-0.1.0-py3-none-any.whl
```

## Usage Example


Let's walk through a complete example with a `User` and `Post` relationship.

### 1\. Define Your SQLModels

```python
from typing import List, Optional
from danticsql import DanticSQL
from sqlmodel import Field, Relationship, SQLModel
import pandas as pd

class User(SQLModel, table=True):
    # NOTE: The column name must be unique in all tables which you want to query.
    # WRONG EXAMPLE: use id instead of user_id
    user_id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    posts: List["Post"] = Relationship(back_populates="user")

class Post(SQLModel, table=True):
    # NOTE: The column name must be unique in all tables which you want to query.
    post_id: Optional[int] = Field(default=None, primary_key=True)
    title: str
    user_id: Optional[int] = Field(default=None, foreign_key="user.user_id")
    user: Optional[User] = Relationship(back_populates="posts")
```

### 2\. Simulate a DataFrame

This DataFrame is what you might get from a `JOIN` query. Note that all column names are unique.

```python
# Note: DanticSQL expects related table PKs to be present.
data = {
    "user_id": [1, 1, 2, 3],
    "name": ["Alice", "Alice", "Bob", "Charlie"],
    "post_id": [101, 102, 103, None],
    "title": ["Post 1 by Alice", "Post 2 by Alice", "Post 1 by Bob", None],
}
df = pd.DataFrame(data, dtype="object")
```

### 3\. Use DanticSQL to Reconstruct Objects

```python
# 1. Initialize DanticSQL with the models and all columns from the query
models = [User, Post]
queried_columns = list(df.columns)
dantic = DanticSQL(models=models, queried_columns=queried_columns)

# 2. Parse the DataFrame into individual SQLModel instances
dantic.pydantic_all(df)

# 3. Connect the instances based on their defined relationships
dantic.connect_all()

# 4. Access your fully formed, nested objects!
instances = dantic.instances

# --- Verification ---
users = instances.get("user", [])
for user in users:
    print(f"User: {user.name} (ID: {user.user_id})")
    if user.posts:
        for post in user.posts:
            # The back-reference is also populated!
            print(f"  - Post: '{post.title}' (ID: {post.post_id}), User: {post.user.name}")
    else:
        print("  - No posts found.")
```

### Expected Output:

```
User: Alice (ID: 1)
  - Post: 'Post 1 by Alice' (ID: 101), User: Alice
  - Post: 'Post 2 by Alice' (ID: 102), User: Alice
User: Bob (ID: 2)
  - Post: 'Post 1 by Bob' (ID: 103), User: Bob
User: Charlie (ID: 3)
  - No posts found.
```

-----

## Important Considerations: Unique Field Naming

**Core Requirement:** All column names in the input DataFrame passed to DanticSQL **must be unique**.

When you `JOIN` multiple tables, it's common to have conflicting column names like `id` or `created_at`. If duplicate column names exist, DanticSQL cannot determine which model the column belongs to.

### Recommended Solution: Use a Database View

For Text-to-SQL applications, using a database `VIEW` is the highly recommended approach to solve this. A view is a virtual table based on the result-set of an SQL statement. You can create a view that encapsulates all the necessary `JOIN` logic and, crucially, **renames columns** to ensure uniqueness.

For example, you could create a view to join `user` and `post` tables like this:

```sql
CREATE VIEW user_posts_view AS
SELECT
    u.id AS user_id,       -- Ensure the name is unique
    u.name AS user_name,
    p.id AS post_id,       -- Ensure the name is unique
    p.title AS post_title,
    p.user_id AS user_id -- The foreign key column must be present
FROM
    "user" u
LEFT JOIN
    post p ON u.id = p.user_id;
```

**Why is this effective in Text-to-SQL?**

1.  **Stability:** It provides a stable and clearly structured data source for the LLM. Even if the underlying tables are complex, the view presents a simplified interface.
2.  **Flexibility:** The data requirements for a Text-to-SQL application can change frequently. Modifying a view's definition is far easier and safer than altering the physical database schema.
3.  **Decoupling:** It decouples the data model that the LLM queries from the database's physical storage, making your application more robust and easier to maintain.

By querying this view (`SELECT * FROM user_posts_view;`), the resulting DataFrame will have unique column names, ready to be processed directly by DanticSQL.

-----

## API Overview

### `class DanticSQL`

The main class that orchestrates the process.

  * `__init__(self, models: list[type[SQLModel]], queried_columns: list[str])`

      * **`models`**: A list of the `SQLModel` classes you expect to parse from the data.
      * **`queried_columns`**: A list of all column names from the input `DataFrame`. This is used to ensure the primary keys required for parsing are present.

  * `pydantic_all(self, records: pd.DataFrame)`

      * Parses the `DataFrame` and creates de-duplicated model instances. Populates an internal `_instances` dictionary.

  * `connect_all(self)`

      * Connects the instances created by `pydantic_all` by setting their `Relationship` attributes.

  * `instances` (property)

      * Returns the final result: a dictionary where keys are table names and values are lists of the fully reconstructed `SQLModel` objects.

-----
## Motivation: Bridging the Gap in Text-to-SQL

DanticSQL was born out of a practical challenge encountered while developing modern **Text-to-SQL** applications. Large Language Models (LLMs) are incredibly effective at translating natural language questions into complex SQL queries. However, a significant gap exists between the output of these models and the structured, object-oriented world of modern Python applications.

LLMs generate raw SQL strings. When you execute these queries—especially complex ones with `JOINs`—you get a flat, tabular result, typically as a Pandas DataFrame. While this data is correct, it doesn't integrate with the rich object models (like `SQLModel` or `Pydantic` models) that developers use to build robust applications. The benefits of using an ORM, such as automatic data validation, type hinting, and clearly defined relationships (`user.posts`), are lost at this stage.

I first developed the core logic of DanticSQL to solve this very problem in my own Text-to-SQL project. The goal was to allow the application to leverage the dynamic query generation of an LLM while still benefiting from the clean, maintainable, and type-safe data structures provided by `SQLModel`. After realizing this was a common hurdle, I decided to extract and refine the code into this standalone library.

**DanticSQL is the bridge.** It takes the raw, flat output from your LLM-generated query and intelligently reconstructs it into the nested `SQLModel` object graphs your application is designed to work with. This allows you to combine the power of natural language querying with the elegance and safety of an object-relational mapper.

-----

## Dependencies

  * `pandas`
  * `sqlalchemy`
  * `sqlmodel`
  * `pydantic`
