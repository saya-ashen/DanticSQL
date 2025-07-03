from typing import Any, ClassVar, Literal
from sqlmodel import SQLModel as SQLModelBase

class SQLModel(SQLModelBase):
    _primary_key_field_names: ClassVar[list[str]] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        当这个类的任何子类被定义时，这个方法就会运行。
        它会检查新定义的模型，并缓存其主键字段的名称。
        """
        super().__init_subclass__(**kwargs)

        # 仅为具有 __table__ 的模型（即 table=True 的模型）执行此操作
        if not hasattr(cls, "__table__"):
            # 如果一个非表模型继承了它，我们不需要做任何事，但也不应该让它可哈希。
            # 为了安全起见，可以清空字段名列表。
            cls._primary_key_field_names = []
            return

        try:
            # 使用 SQLAlchemy 的 inspect API 获取主键列
            pk_columns = inspect(cls).primary_key
            pk_field_names = [col.key for col in pk_columns if col.key]

            if not pk_field_names:
                raise ValueError(
                    f"类 '{cls.__name__}' 继承自可哈希的 SQLModel, 但无法识别出主键字段。"
                    f"请确保模型使用 `table=True` 定义，并且至少有一个字段标记为 `primary_key=True`。"
                )
            
            # 对主键字段名进行排序，以确保哈希值的一致性
            cls._primary_key_field_names = sorted(pk_field_names)

        except (NoInspectionAvailable, AttributeError) as e:
            raise TypeError(
                f"在为类 '{cls.__name__}' 设置哈希功能时出错。无法检查其主key。"
                f"原始错误: {e}"
            ) from e

    def _get_primary_key_field_names(self, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        pk_field_names: list[str] = []

        # 检查类是否具有 __table__ 属性，这表明它是一个表模型。
        # SQLModel 会为 table=True 的模型设置此属性。
        cls = self.__class__
        if hasattr(cls, "__table__"):
            try:
                # 访问 SQLAlchemy 表的主键定义。
                # cls.__table__ 的类型是 sqlalchemy.schema.Table。
                # type: ignore[attr-defined] 用于忽略 __table__ 在静态分析中可能未知的具体类型。
                pk_columns = cls.__table__.primary_key.columns  # type: ignore[attr-defined]
                for col in pk_columns:
                    if col.key:  # col.key 是映射类上的属性名称
                        pk_field_names.append(col.key)
                    # else: 一个没有 .key 的主键列，在标准的 SQLModel 用法中应该很少见或不可能。
            except NoInspectionAvailable:
                # 如果 __table__ 存在但 SQLAlchemy 尚不能完全检查它，则可能发生此情况。
                # 这将导致 pk_field_names 保持为空。
                pass
            except AttributeError:
                # 处理 __table__ 存在但 primary_key 或 columns 属性丢失的情况，
                # 或者 __table__ 本身不是我们期望的类型。
                # 这也将导致 pk_field_names 保持为空。
                pass
        # else: 类不是一个表模型（没有 __table__ 属性）。
        # 对于这种基于数据库主键的哈希方案，不支持非表模型。

        if not pk_field_names:
            raise ValueError(
                f"类 '{cls.__name__}' 继承自 SQLModel，但无法从其 `__table__` 元数据中 "
                f"识别出任何主键字段。请确保模型是用 `table=True` 定义的，并且具有用 "
                f"`sqlmodel.Field(primary_key=True)` 标记的字段。"
            )

        # 对主键字段名称进行排序，以确保无论在模型中定义的顺序如何，哈希值都保持一致。
        cls._primary_key_field_names = sorted(pk_field_names)

    def _get_primary_key_values_tuple(self) -> tuple[Any, ...]:
        """
        以元组形式检索实例的主键字段值。
        元组中值的顺序基于主键字段名称的排序顺序。
        """
        if len(self._primary_key_field_names) == 0:
            self._get_primary_key_field_names()
            assert len(self._primary_key_field_names) > 0
        values = [getattr(self, field_name) for field_name in self._primary_key_field_names]
        return tuple(values)

    def __hash__(self) -> int:
        # 如果 __init_subclass__ 失败，类定义本身就会引发错误。
        # 因此，_primary_key_field_names 保证是一个非空的字符串列表。
        pk_values = self._get_primary_key_values_tuple()
        return hash(pk_values)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True

        # 仅与相同具体类的实例进行比较。
        # 这确保 `other` 也具有相同的 `_primary_key_field_names`
        # 和 `_get_primary_key_values_tuple` 方法。
        if type(self) is not type(other):
            return NotImplemented

        # 此时，`other` 是与 `self`相同类的实例。
        self_pk_values = self._get_primary_key_values_tuple()

        # 我们知道 `other` 是相同类型，所以它有这个方法。
        # 如果静态检查器抱怨 'object' 没有此方法，请添加类型忽略。
        other_pk_values = other._get_primary_key_values_tuple()  # type: ignore[attr-defined]

        return self_pk_values == other_pk_values

    def __repr__(self) -> str:
        # __init_subclass__ 确保 _primary_key_field_names 存在。
        # 如果由于某种原因（例如，直接实例化HashableSQLModel而不是子类化）它不存在，
        # 则需要一个回退。但对于正确使用的子类，它总是存在的。
        if not hasattr(self.__class__, "_primary_key_field_names") or not self.__class__._primary_key_field_names:
            return f"<{self.__class__.__name__}(primary_keys_not_initialized)>"

        pk_values_str_list = []
        for name in self._primary_key_field_names:
            try:
                value = getattr(self, name)
                pk_values_str_list.append(f"{name}={value!r}")
            except AttributeError:
                pk_values_str_list.append(f"{name}=<error_missing>")

        pk_values_str = ", ".join(pk_values_str_list)
        return f"<{self.__class__.__name__}({pk_values_str})>"

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        model_dump = super().model_dump(*args, **kwargs)

        if kwargs.get("by_alias") is False:
            return model_dump

        for field_name, field_info in self.__fields__.items():
            if field_info.alias and field_name in model_dump.keys():  # type: ignore
                model_dump[field_info.alias] = model_dump.pop(field_name)  # type: ignore

        return model_dump

