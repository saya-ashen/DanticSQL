from typing import Any

import pandas as pd
from pydantic import TypeAdapter
from sqlalchemy import inspect

from sqlmodel import SQLModel


class DanticSQL:
    """
    Handles the conversion of a flat pandas DataFrame (from a SQL join)
    into a nested structure of SQLModel/Pydantic objects, correctly
    reconstructing their relationships.
    """

    def __init__(self, models: list[type[SQLModel]], queried_columns: list[str]):
        """
        Initializes the DanticSQL processor.

        Args:
            models: A list of all SQLModel classes involved in the query.
            queried_columns: A list of column names present in the result DataFrame.
        """
        self.models = models
        self.queried_columns = queried_columns
        self.table_primary_key_map: dict[type[SQLModel], Any] = {}
        self._instances: dict[type[SQLModel], list[SQLModel]] = {}

        # Stores intermediate data for connecting relationships later.
        # Format: {
        #   source_table_type: {
        #     related_table_type: "foreign_key_column_name",
        #     "data": DataFrame with source_pk and all foreign_key_columns
        #   }
        # }
        self.relationship_records_map: dict[type[SQLModel], dict] = {}

        # Pre-process models to find their primary keys.
        for table in models:
            skip_current_table = False
            inspector = inspect(table.__table__)  # type: ignore

            # A table is only processed if its primary key columns are in the query result.
            for primary_key in inspector.primary_key.columns:
                if primary_key.name not in self.queried_columns:
                    skip_current_table = True
                    break
            if skip_current_table:
                continue

            # Supports only single-column primary keys for mapping.
            if len(inspector.primary_key.columns) == 1:
                self.table_primary_key_map[table] = inspector.primary_key.columns[0]
            else:
                self.table_primary_key_map[table] = None

    @property
    def instances(self) -> dict[str, list[SQLModel]]:
        """
        Returns the processed instances, keyed by table name instead of class type.
        """
        instances = {}
        for table, instance_list in self._instances.items():
            instances[table.__tablename__] = instance_list
        return instances

    def get_model_by_table(self, table):
        """Helper to find a SQLModel class from a SQLAlchemy Table object."""
        for _table in self.models:
            if _table.__tablename__ == table.name:
                return _table
        return None

    def pydantic_all(self, records: pd.DataFrame):
        """
        Processes the entire DataFrame to create Pydantic instances for all models.
        """
        instances = {}
        for table in self.table_primary_key_map.keys():
            instances[table] = self.pydantic_all_single_table(records, table)
        self._instances = instances

    def pydantic_all_single_table(self, records: pd.DataFrame, table: type[SQLModel]):
        """
        Creates unique Pydantic instances for a single table from the DataFrame,
        aggregating the keys of any related models for later connection.
        """
        inspector = inspect(table)
        relationships = inspector.relationships
        all_related_table_keys = []
        self.relationship_records_map[table] = {}

        # Identify all foreign key columns based on the model's relationships.
        for relationship in relationships:
            related_table = self.get_model_by_table(relationship.target)
            if not related_table:
                continue

            # For MANYTOONE and ONETOMANY, the key is the related table's primary key.
            if relationship.direction.name in ("MANYTOONE", "ONETOMANY"):
                # Assumes single-column PK on the related table.
                related_table_primary_key = related_table.__table__.primary_key.columns[0].name  # type: ignore
                all_related_table_keys.append(related_table_primary_key)
                self.relationship_records_map[table][related_table] = related_table_primary_key
            # For MANYTOMANY, the key is on the association table.
            elif relationship.direction.name == "MANYTOMANY":
                pairs = relationship.local_remote_pairs
                assert pairs and len(pairs) == 2, "Many-to-many relationship must have exactly two pairs."
                target_attr_name_on_association = pairs[1][0].name
                all_related_table_keys.append(target_attr_name_on_association)
                self.relationship_records_map[table][related_table] = target_attr_name_on_association

        # Find which of the identified foreign key columns are actually in the DataFrame.
        df_columns = set(records.columns)
        intersection_list = list(df_columns.intersection(set(all_related_table_keys)))

        primary_key_names = [pk.name for pk in inspect(table).primary_key]

        # Define aggregation logic for groupby.
        agg_dict = {col: "first" for col in records.columns if col not in intersection_list + primary_key_names}

        def _aggregate_keys(x) -> set | Any:
            """
            Aggregates foreign keys. If only one unique key exists (after dropping NAs),
            it's a to-one relationship. Otherwise, collect all unique keys for a to-many relationship.
            """
            unique_keys = set(x.dropna())
            if len(unique_keys) == 1:
                return unique_keys.pop()
            return unique_keys

        agg_dict.update({key: _aggregate_keys for key in intersection_list})

        # Group by the primary key of the current table to create unique records.
        # Other columns are picked (`first`) and related keys are aggregated.
        if len(agg_dict) > 1:
            unique_records = records.groupby(primary_key_names).agg(agg_dict).reset_index()
        else:
            unique_records = records.drop_duplicates(subset=primary_key_names)

        # Store the dataframe containing only PKs and aggregated FKs for the connect phase.
        self.relationship_records_map[table]["data"] = unique_records[primary_key_names + intersection_list]

        # Convert the cleaned, unique records into Pydantic model instances.
        unique_records_dict = unique_records.to_dict(orient="records")
        list_adapter = TypeAdapter(list[table])
        instances = list_adapter.validate_python(unique_records_dict)

        return instances

    def connect_all(self):
        """
        Connects all pydantic instances together based on their relationships.
        This optimized version pre-calculates all necessary lookup maps to avoid
        redundant work inside loops.
        """
        # Create a lookup map for every model type: {pk_value: instance}.
        all_instance_maps: dict[type[SQLModel], dict[Any, SQLModel]] = {}
        table_primary_key_name_map: dict[type[SQLModel], str] = {}

        # Pre-build primary key name and instance lookup maps for all models.
        for table, instances in self._instances.items():
            if not instances:
                continue
            pk_cols = [col.name for col in inspect(table.__table__).primary_key.columns]  # type: ignore
            # This connection logic only works for single-column primary keys.
            if len(pk_cols) != 1:
                continue
            pk_name = pk_cols[0]
            table_primary_key_name_map[table] = pk_name
            all_instance_maps[table] = {getattr(inst, pk_name): inst for inst in instances}

        # Iterate through the relationship data we prepared earlier.
        for source_table, rel_map in self.relationship_records_map.items():
            source_pk_name = table_primary_key_name_map.get(source_table)
            source_instance_map = all_instance_maps.get(source_table)

            if not source_pk_name or not source_instance_map:
                continue

            relationship_df = rel_map.get("data")
            if relationship_df is None or relationship_df.empty:
                continue

            # Pre-fetch all relationship definitions for the source model.
            relationships_by_target_model = {
                self.get_model_by_table(rel.target): rel for rel in inspect(source_table).relationships
            }

            # Iterate through the relationship data rows.
            for row in relationship_df.to_dict(orient="records"):
                source_pk_value = row.get(source_pk_name)
                source_instance = source_instance_map.get(source_pk_value)

                if not source_instance:
                    continue

                # Now, link this source instance to its related instances.
                for related_table, related_key_col in rel_map.items():
                    if related_table == "data":
                        continue

                    related_instance_map = all_instance_maps.get(related_table)
                    if not related_instance_map:
                        continue

                    relationship_to_set = relationships_by_target_model.get(related_table)
                    if not relationship_to_set:
                        continue

                    attribute_name = relationship_to_set.key
                    direction = relationship_to_set.direction.name

                    # Get the foreign key value(s) from the aggregated row.
                    related_pk_values = row.get(related_key_col, set())
                    if not isinstance(related_pk_values, set):
                        related_pk_values = {related_pk_values}

                    # Efficiently look up all related instances from the pre-built map.
                    objects_to_link = [
                        related_instance_map.get(pk) for pk in related_pk_values if pk in related_instance_map
                    ]

                    # Set the relationship attribute on the source instance.
                    if direction in ("ONETOMANY", "MANYTOMANY"):
                        # Check if the relationship is a list (e.g., users) or a single object (e.g., profile).
                        if relationship_to_set.uselist:
                            setattr(source_instance, attribute_name, objects_to_link)
                        else:  # Handle one-to-one as a special case of to-many.
                            setattr(source_instance, attribute_name, objects_to_link[0] if objects_to_link else None)
                    elif direction == "MANYTOONE":
                        setattr(source_instance, attribute_name, objects_to_link[0] if objects_to_link else None)
