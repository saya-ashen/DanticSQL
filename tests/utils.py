from sqlmodel import SQLModel


# 判断两个pydantic对象是否完全相同
def compare_pydantic_objects(obj1:SQLModel, obj2:SQLModel):
    """Compare two Pydantic objects for equality."""
    if obj1.__class__ != obj2.__class__:
        return False
    return obj1.model_dump()  == obj2.model_dump()


# 判断两个dict[str,List[SQLModel]]对象是否完全相同
def compare_pydantic_dicts(dict1: dict[str, list[SQLModel]], dict2: dict[str, list[SQLModel]]):
    """Compare two dictionaries of Pydantic objects for equality."""
    if dict1.keys() != dict2.keys():
        print(f"Keys not equal: {dict1.keys()} != {dict2.keys()}")
        return False
    for key in dict1:
        if len(dict1[key]) != len(dict2[key]):
            print(f"Length not equal for key '{key}': {len(dict1[key])} != {len(dict2[key])}")
            return False
        for obj1, obj2 in zip(dict1[key], dict2[key], strict=False):
            if not compare_pydantic_objects(obj1, obj2):
                print(f"Objects not equal: {obj1} != {obj2}")
                return False
    return True
