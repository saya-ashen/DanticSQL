from typing import Any, ClassVar, Literal
import pandas as pd
from pydantic import TypeAdapter
from sqlalchemy import inspect
from . import SQLModel


class DanticSQL:
    def __init__(self, models: list[type[SQLModel]], queried_columns: list[str]):
        self.models = models
        self.table_primary_key_map: dict[type[SQLModel], Any] = {}
        self.queried_columns = queried_columns
        for table in models:
            skip_current_table = False
            inspector = inspect(table.__table__)  # type: ignore
            # 判断是否有主键列,如果没有主键列，则跳过当前表
            for primary_key in inspector.primary_key.columns:
                if primary_key.name not in self.queried_columns:
                    skip_current_table = True
                    break
            if skip_current_table:
                continue
            if len(inspector.primary_key.columns) == 1:
                self.table_primary_key_map[table] = inspector.primary_key.columns[0]
            else:
                self.table_primary_key_map[table] = None
        self._instances: dict[type[SQLModel], list[SQLModel]] = {}

        # 存储pydantic过程的中间结果，方便后续进行连接
        self.relationship_records_map: dict[type[SQLModel], dict] = {}

    @property
    def instances(self) -> dict[str, list[SQLModel]]:
        instances = {}
        for table, instance_dict in self._instances.items():
            instances[table.__tablename__] = instance_dict
        return instances

    def get_model_by_table(self, table):
        for _table in self.models:
            if _table.__tablename__ == table.name:
                return _table
        raise ValueError(f"Table {table.name} not found in all_tables")

    def pydantic_all(self, records: pd.DataFrame):
        instances = {}
        for table in self.table_primary_key_map.keys():
            instances[table] = self.pydantic_all_single_table(records, table)
        self._instances = instances

    def pydantic_all_single_table(self, records: pd.DataFrame, table: type[SQLModel]):
        """
        使用多进程并行处理数据
        """

        # 获取table 中的所有主键
        table_primary_key_name_map: dict[type[SQLModel], list[str]] = {}
        for _table in self.models:
            inspector = inspect(_table.__table__)  # type: ignore
            table_primary_key_name_map[_table] = [col.name for col in inspector.primary_key.columns]

        inspector = inspect(table)
        relationships = inspector.relationships
        all_related_table_keys = []
        self.relationship_records_map[table] = {}
        for relationship in relationships:
            assert relationship.local_remote_pairs is not None, "Relationship must have local_remote_pairs defined."
            relationship_direction = relationship.direction.name
            related_table = self.get_model_by_table(relationship.target)
            if relationship_direction == "MANYTOONE":
                related_table_primary_key = related_table.__table__.primary_key.columns[0].name  # type: ignore
                all_related_table_keys.append(related_table_primary_key)
                self.relationship_records_map[table][related_table] = related_table_primary_key
            elif relationship_direction == "ONETOMANY":
                related_table_primary_key = related_table.__table__.primary_key.columns[0].name  # type: ignore
                all_related_table_keys.append(related_table_primary_key)
                self.relationship_records_map[table][related_table] = related_table_primary_key
            elif relationship_direction == "MANYTOMANY":
                pairs = relationship.local_remote_pairs
                assert pairs is not None and len(pairs) == 2, "Many-to-many relationship must have exactly two pairs."

                target_attr_name_on_association = pairs[1][0].name
                all_related_table_keys.append(target_attr_name_on_association)
                self.relationship_records_map[table][related_table] = target_attr_name_on_association

        df_columns = set(records.columns)
        all_related_table_keys_set = set(all_related_table_keys)
        intersection = df_columns.intersection(all_related_table_keys_set)
        intersection_list = list(intersection)

        primary_key_names = table_primary_key_name_map[table]
        agg_dict = {col: "first" for col in records.columns if col not in intersection_list + primary_key_names}
        agg_dict.update(dict.fromkeys(intersection_list, lambda x: set(x.dropna())))
        if len(primary_key_names) > 1:
            unique_records = records.groupby(primary_key_names).agg(agg_dict).reset_index()
        else:
            unique_records = records.drop_duplicates(subset=primary_key_names)

        relationship_records = unique_records[primary_key_names + intersection_list]
        self.relationship_records_map[table]["data"] = relationship_records
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
        # FIXME: 这部分连接时有重复问题，需要检查哪里的问题

        # 1. Create lookup maps for ALL instances of ALL models first.
        # This is the most significant optimization.
        # e.g., all_instance_maps = {
        #     User: {'user_pk_1': user_obj_1, 'user_pk_2': user_obj_2},
        #     Post: {'post_pk_1': post_obj_1, 'post_pk_2': post_obj_2}
        # }
        all_instance_maps: dict[type[SQLModel], dict[Any, SQLModel]] = {}
        table_primary_key_name_map: dict[type[SQLModel], str] = {}

        for table, instances in self._instances.items():
            if not instances:
                continue

            inspector = inspect(table.__table__)  # type: ignore
            pk_cols = [col.name for col in inspector.primary_key.columns]

            # This logic currently only supports single-column PKs for relationships
            if len(pk_cols) != 1:
                # You might want to log this or handle it more gracefully
                continue

            pk_name = pk_cols[0]
            table_primary_key_name_map[table] = pk_name
            all_instance_maps[table] = {getattr(inst, pk_name): inst for inst in instances}

        # 2. Iterate through the relationship data to link objects.
        for source_table, rel_map in self.relationship_records_map.items():
            source_pk_name = table_primary_key_name_map.get(source_table)
            source_instance_map = all_instance_maps.get(source_table)

            # Skip if there's no PK or no instances for the source table
            if not source_pk_name or not source_instance_map:
                continue

            relationship_df = rel_map.get("data")
            if relationship_df is None or relationship_df.empty:
                continue

            # Get all relationships for the source_table once
            source_inspector = inspect(source_table)
            relationships_by_target_model = {
                self.get_model_by_table(rel.target): rel for rel in source_inspector.relationships
            }

            # 3. Use a much faster iteration method than iterrows()
            for row in relationship_df.to_dict(orient="records"):
                source_pk_value = row.get(source_pk_name)
                source_instance = source_instance_map.get(source_pk_value)

                if not source_instance:
                    continue

                # 4. Iterate through the defined relationships for this table
                for related_table, related_key_col in rel_map.items():
                    if related_table == "data":
                        continue

                    # Get the pre-built lookup map for the related instances
                    related_instance_map = all_instance_maps.get(related_table)
                    if not related_instance_map:
                        continue

                    relationship_to_set = relationships_by_target_model.get(related_table)
                    if not relationship_to_set:
                        continue

                    attribute_name = relationship_to_set.key
                    direction = relationship_to_set.direction.name

                    # Get the list of FKs from the row
                    related_pk_values: set = row.get(related_key_col, set())
                    if not isinstance(related_pk_values, set):
                        related_pk_values = set([related_pk_values])

                    # Find all related objects in one go using the map.
                    # This is much faster than looping and getting one by one.
                    try:
                        objects_to_link = [
                            related_instance_map.get(pk) for pk in related_pk_values if pk in related_instance_map
                        ]
                    except Exception:
                        import pdb

                        pdb.set_trace()

                    # Set the attribute on the source instance
                    if direction in ("ONETOMANY", "MANYTOMANY"):
                        setattr(source_instance, attribute_name, objects_to_link)
                    elif direction == "MANYTOONE":
                        setattr(source_instance, attribute_name, objects_to_link[0] if objects_to_link else None)

