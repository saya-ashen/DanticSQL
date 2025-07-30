from typing import Any
import copy

import pandas as pd
from pydantic import TypeAdapter
from sqlalchemy import inspect

from sqlmodel import SQLModel
import collections
from sqlmodel._compat import SQLModelConfig
from sqlmodel import Field,  SQLModel
from dataclasses import dataclass
from typing import List, Type, Dict, Set, DefaultDict, Any
from collections import defaultdict

class Extra(SQLModel,table=True):
    __table_args__ = ({"info": {"title": "Extra" }},)
    model_config = SQLModelConfig(extra="allow")
    id: int|None = Field(default=None, primary_key=True)

@dataclass
class CteGenerationResult:
    """封装CTE生成结果，包括SQL、映射规则和冲突信息。"""
    sql_string: str
    # 映射字典的结构: { '最终列名': {'table': '原始表名', 'column': '原始列名'} }
    mapping: Dict[str, Dict[str, str]] 
    conflicting_columns: Set[str]

def generate_cte_with_mapping(models: List[Type[SQLModel]]) -> CteGenerationResult:
    """
    生成CTE SQL，并返回一个用于恢复列名和表名的映射规则。
    """
    if not models or len(models) < 2:
        return CteGenerationResult("", {}, set())

    # 步骤1: 深度检查 (与上一版相同)
    column_occurrences: DefaultDict[str, List[bool]] = collections.defaultdict(list)
    model_columns_info: Dict[str, List[Dict]] = {}
    for model in models:
        table_name = model.__table__.name
        columns_info = []
        for column in model.__table__.columns:
            is_foreign_key = bool(column.foreign_keys)
            column_occurrences[column.name].append(is_foreign_key)
            columns_info.append({"name": column.name})
        model_columns_info[table_name] = columns_info

    # 步骤2: 识别“真实冲突” (与上一版相同)
    real_conflicts: Set[str] = set()
    for name, is_fk_list in column_occurrences.items():
        if len(is_fk_list) > 1 and not any(is_fk_list):
            real_conflicts.add(name)

    # 如果没有真实冲突，返回空结果
    if not real_conflicts:
        return CteGenerationResult("", {}, set())

    # 步骤3: 生成CTE和映射规则
    cte_definitions = []
    final_mapping: Dict[str, Dict[str, str]] = {}

    for model in models:
        table_name = model.__table__.name
        cte_name = f"aliased_{table_name}"
        
        aliased_columns = []
        for column_info in model_columns_info[table_name]:
            column_name = column_info["name"]
            
            if column_name in real_conflicts:
                final_column_name = f"{table_name}_{column_name}"
                aliased_columns.append(f"{table_name}.{column_name} AS {final_column_name}")
                # 记录映射：别名 -> {原始表, 原始列}
                final_mapping[final_column_name] = {"table": table_name, "column": column_name}
            else:
                final_column_name = column_name
                aliased_columns.append(f"{table_name}.{column_name}")
                # 记录映射：原名 -> {原始表, 原始列}
                final_mapping[final_column_name] = {"table": table_name, "column": column_name}
        
        select_clause = ",\n         ".join(aliased_columns)
        cte_def = (
            f"{cte_name} AS (\n"
            f"    SELECT \n         {select_clause}\n"
            f"    FROM {table_name}\n"
            f")"
        )
        cte_definitions.append(cte_def)
        
    sql_string = "WITH\n" + ",\n".join(cte_definitions)
    
    return CteGenerationResult(sql_string, final_mapping, real_conflicts)

def restore_row_to_nested_dict(
    row: Dict[str, Any], 
    mapping: Dict[str, Dict[str, str]]
) -> Dict[str, Dict[str, Any]]:
    """
    根据映射规则，将单行扁平的查询结果恢复为按表名组织的嵌套字典。

    :param row: 从数据库查询到的一行数据（字典），可能包含别名。
    :param mapping: 由 generate_with_mapping 函数生成的映射规则。
    :return: 一个嵌套字典，格式为 {'table_name': {'col1': val1, 'col2': val2}, ...}
    """
    nested_result = defaultdict(dict)
    for final_column_name, value in row.items():
        if final_column_name in mapping:
            origin_info = mapping[final_column_name]
            table = origin_info['table']
            column = origin_info['column']
            nested_result[table][column] = value
    return dict(nested_result)

def transform_schema_for_llm(
    original_schema: Dict[str, Any],
    mapping: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    """
    根据映射规则，重命名一个自定义schema结构中的表名和列名。

    :param original_schema: 用户自定义的、按表分组的schema字典。
    :param generation_result: 调用generate_with_mapping返回的结果对象。
    :return: 一个表名和列名都被重映射过的新schema字典。
    """
    # 如果没有生成CTE（没有冲突），则无需做任何改变，直接返回原结构的深拷贝
    if not mapping:
        return original_schema

    # 步骤1: 创建一个反向映射以便于查找: (table, column) -> final_name
    inverted_mapping = {
        (origin['table'], origin['column']): final_name
        for final_name, origin in mapping.items()
    }

    remapped_schema = {}
    
    # 步骤2: 遍历原始的schema结构
    for table_name, table_info in original_schema.items():
        # 复制一份以防修改原始输入
        new_table_info = copy.deepcopy(table_info)
        original_columns = new_table_info.get("columns", {})
        new_columns = {} # 准备一个新字典来存储结果

        for original_col_name, column_info in original_columns.items():
            # 使用反向映射查找新的列名
            final_col_name = inverted_mapping.get((table_name, original_col_name))
            
            # 如果找到了新的列名，就用新的名字作为键；否则，保留原名
            if final_col_name:
                new_columns[final_col_name] = column_info
            else:
                new_columns[original_col_name] = column_info

        new_table_info["columns"] = new_columns
        
        new_table_name = f"aliased_{table_name}"
        remapped_schema[new_table_name] = new_table_info

    return remapped_schema
class DanticSQL:
    """
    处理从SQL连接查询得到的扁平化pandas DataFrame，
    并将其高效地转换为嵌套的、包含正确关系的SQLModel/Pydantic对象结构。
    
    该实现采用“行中心”模式：
    1. 使用 `restore_row_to_nested_dict` 预处理每一行数据。
    2. 在一次遍历中创建所有数据表的唯一对象实例，并将其缓存。
    3. 在第二次遍历中，根据缓存的实例和关系元数据，连接所有对象。
    """

    def __init__(self, models: List[Type[SQLModel]], queried_columns: List[str], mapping: Dict[str, Dict[str, str]]):
        """
        初始化 DanticSQL 处理器。

        Args:
            models: 查询涉及的所有 SQLModel 类。
            queried_columns: 结果 DataFrame 中存在的列名列表。
            mapping: 由 generate_cte_with_mapping 生成的列映射规则。
        """
        self.models = models
        self.mapping = mapping
        self.queried_columns = queried_columns

        # --- 预处理，方便快速查找 ---
        # 1. 表名 -> 模型类的映射
        self.model_map: Dict[str, Type[SQLModel]] = {m.__tablename__: m for m in models}
        
        # 2. 模型类 -> 主键名的映射 (仅支持单主键)
        self.pk_map: Dict[Type[SQLModel], str | None] = {}
        for model in models:
            pk_cols = [col.name for col in inspect(model.__table__).primary_key.columns]  # type: ignore
            self.pk_map[model] = pk_cols[0] if len(pk_cols) == 1 else None

        # 3. 通过映射关系，高效计算出不属于任何模型的“额外”列
        model_columns = set(self.mapping.keys())
        self.extra_columns = [col for col in self.queried_columns if col not in model_columns]

        # --- 内部状态 ---
        # 实例缓存: {ModelClass: {pk_value: instance}}
        self._instance_cache: Dict[Type[SQLModel], Dict[Any, SQLModel]] = defaultdict(dict)
        self._processed = False

    @property
    def instances(self) -> Dict[str, List[SQLModel]]:
        """
        以字典形式返回所有处理过的、按表名分类的实例列表。
        """
        if not self._processed:
            # 这是一个安全措施，防止在处理数据前访问实例
            return {}
            
        output: Dict[str, List[SQLModel]] = defaultdict(list)
        for model_cls, instance_map in self._instance_cache.items():
            output[model_cls.__tablename__] = list(instance_map.values())
        return dict(output)

    def get_model_by_table_name(self, table_name: str) -> Type[SQLModel] | None:
        """根据表名获取模型类。"""
        return self.model_map.get(table_name)

    def process_df(self, df: pd.DataFrame) -> None:
        """
        处理整个DataFrame，完成实例创建和关系连接。
        这是该类的主要入口点。
        """
        if df.empty:
            self._processed = True
            return

        # 步骤 1: 从扁平数据创建所有唯一的模型实例并缓存
        nested_rows = self._create_instances_from_df(df)

        # 步骤 2: 连接缓存中的实例之间的关系
        self._connect_instances(nested_rows)
        
        # 步骤 3: (可选) 处理不属于任何模型的额外数据
        if self.extra_columns:
            self._process_extra_data(df)

        self._processed = True

    def _create_instances_from_df(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        遍历DataFrame，为每一行调用restore_row_to_nested_dict，
        并创建所有唯一的、无关联的SQLModel实例，存入缓存。
        """
        nested_rows = []
        for row in df.to_dict(orient="records"):
            nested_row = restore_row_to_nested_dict(row, self.mapping)
            nested_rows.append(nested_row)

            for table_name, data in nested_row.items():
                model_cls = self.get_model_by_table_name(table_name)
                if not model_cls:
                    continue
                
                pk_name = self.pk_map.get(model_cls)
                if not pk_name:
                    continue # 不处理无主键或复合主键的模型

                pk_val = data.get(pk_name)
                if pk_val is not None and pk_val not in self._instance_cache[model_cls]:
                    # 如果实例不在缓存中，则创建并添加
                    instance = model_cls.model_validate(data)
                    self._instance_cache[model_cls][pk_val] = instance
        
        return nested_rows

    def _connect_instances(self, nested_rows: List[Dict[str, Any]]):
        """
        基于缓存中的实例和原始行数据，建立对象间的关系。
        """
        # 步骤 2a: 初始化所有实例的 'to-many' 关系属性为空列表
        for cache_per_model in self._instance_cache.values():
            for instance in cache_per_model.values():
                for rel in inspect(instance.__class__).relationships:
                    if rel.uselist: # 如果是 to-many 关系
                        setattr(instance, rel.key, [])

        # 步骤 2b: 遍历结构化行数据，填充关系
        for nested_row in nested_rows:
            for source_table, source_data in nested_row.items():
                source_model = self.get_model_by_table_name(source_table)
                if not source_model or not self.pk_map.get(source_model):
                    continue

                source_pk = source_data.get(self.pk_map[source_model])
                source_instance = self._instance_cache[source_model].get(source_pk)
                if not source_instance:
                    continue

                # 遍历此模型的所有关系定义
                for rel in inspect(source_model).relationships:
                    target_model = rel.mapper.class_
                    target_table_name = target_model.__tablename__
                    
                    target_data = nested_row.get(target_table_name)
                    if not target_data or not self.pk_map.get(target_model):
                        continue

                    target_pk = target_data.get(self.pk_map[target_model])
                    target_instance = self._instance_cache[target_model].get(target_pk)
                    if not target_instance:
                        continue
                    
                    # 根据uselist属性决定是赋值还是追加
                    if rel.uselist:
                        # 'to-many' 关系: 向列表中追加
                        getattr(source_instance, rel.key).append(target_instance)
                    else:
                        # 'to-one' 关系: 直接赋值
                        setattr(source_instance, rel.key, target_instance)

         # 步骤 2c: 去除 'to-many' 关系列表中的重复项
        for cache_per_model in self._instance_cache.values():
            for instance in cache_per_model.values():
                for rel in inspect(instance.__class__).relationships:
                    if rel.uselist:
                        original_list = getattr(instance, rel.key)
                        if original_list:
                            # ==================== MODIFIED BLOCK START ====================
                            # Pydantic/SQLModel 对象默认不可哈希，dict.fromkeys 会失败。
                            # 我们应该基于对象的主键来定义唯一性。
                            target_model_cls = rel.mapper.class_
                            pk_name = self.pk_map.get(target_model_cls)

                            # 如果目标模型没有单主键，则无法去重，跳过
                            if not pk_name:
                                continue

                            unique_list = []
                            seen_pks = set()
                            for item in original_list:
                                # 使用 getattr 以防万一 pk_name 不存在于 item 上
                                pk_val = getattr(item, pk_name, None)
                                # 只有当主键存在且未被添加过时，才加入列表
                                if pk_val is not None and pk_val not in seen_pks:
                                    seen_pks.add(pk_val)
                                    unique_list.append(item)
                            
                            setattr(instance, rel.key, unique_list)
    
    def _process_extra_data(self, df: pd.DataFrame):
        """处理不属于任何已定义模型的额外列。"""
        records_dict = df[self.extra_columns].to_dict(orient="records")
        list_adapter = TypeAdapter(List[Extra])
        extra_instances = list_adapter.validate_python(records_dict)
        if extra_instances:
            # 将 Extra 实例存入缓存，以便通过 .instances 属性访问
            self._instance_cache[Extra] = {inst.id: inst for inst in extra_instances if inst.id is not None}
