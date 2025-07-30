import pandas as pd
from danticsql import generate_cte_with_mapping,restore_row_to_nested_dict,transform_schema_for_llm
from typing import List, Type
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select, text

from danticsql import DanticSQL

from typing import Any, List, Optional

class FilmActorLink(SQLModel, table=True):
    actor_id: Optional[int] = Field(default=None, foreign_key="actor.actor_id", primary_key=True)
    film_id: Optional[int] = Field(default=None, foreign_key="film.film_id", primary_key=True)


class Actor(SQLModel, table=True):
    actor_id: int = Field(primary_key=True)
    first_name: str
    last_name: str
    last_update: str = Field(default=None) # 在DB中是datetime, 这里用str简化处理
    
    films: List["Film"] = Relationship(back_populates="actors", link_model=FilmActorLink)

class Film(SQLModel, table=True):
    film_id: int = Field(primary_key=True)
    title: str
    description: Optional[str] = None
    last_update: str

    actors: List[Actor] = Relationship(back_populates="films", link_model=FilmActorLink)


# ==============================================================================
# 3. 测试主逻辑
# ==============================================================================
db_file = "data/sakila.db"
engine = create_engine(f"sqlite:///{db_file}")
def test_sakila():
    """主测试函数"""
    film_ids_to_test = (1, 453, 771)
    cte = generate_cte_with_mapping([FilmActorLink,Film,Actor])
    sql_query = text(f"""
        {cte.sql_string}
        SELECT
            f.film_id,
            f.title,
            f.description,
            f.film_last_update ,  -- 列名冲突处理
            a.actor_id,
            a.first_name,
            a.last_name,
            a.last_update  -- 列名冲突处理
        FROM
            aliased_film f
        LEFT JOIN film_actor fa ON f.film_id = fa.film_id
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
        WHERE f.film_id IN {film_ids_to_test}
        ORDER BY f.film_id, a.actor_id;
    """)

    with Session(engine) as session:
        df_for_dsql = pd.read_sql(sql_query, session.bind)
    

    r = restore_row_to_nested_dict(df_for_dsql.to_dict(orient="records")[0],cte.mapping)
    print(df_for_dsql.head())


    # 定义参与重建的模型
    models_to_process = [Film, Actor]
    # 获取DataFrame的所有列名
    queried_columns = list(df_for_dsql.columns)

    dsql = DanticSQL(models=models_to_process, queried_columns=queried_columns,mapping=cte.mapping)
    
    # 生成所有独立的Pydantic实例
    dsql.process_df(df_for_dsql)
    import IPython;IPython.embed()
    
    # 连接实例之间的关系
    dsql.connect_all()

    # 获取最终重建的嵌套对象
    instances = dsql.instances

    film_map = {film.film_id: film for film in instances.get("film", [])}

    film1 = film_map.get(1)
    assert film1 is not None, "测试失败: 未找到 Film ID 1"
    assert film1.title == "ACADEMY DINOSAUR"
    # 验证关系是列表
    assert isinstance(film1.actors, list), f"测试失败: film.actors 应该是 list, 却是 {type(film1.actors)}"
    # 验证演员数量
    assert len(film1.actors) == 10, f"测试失败: 演员数量应为 10, 却是 {len(film1.actors)}"
    # 验证演员对象类型
    assert isinstance(film1.actors[0], Actor)
    # 验证数据完整性 (包括别名列)
    assert film1.last_update== "2025-07-30 02:56:50" # 这是 film_last_update 的值
    assert film1.actors[0].first_name == "PENELOPE"
    assert film1.actors[0].last_update == "2025-07-30 02:56:49" # 这是 actor_last_update 的值
    print("✅ ACADEMY DINOSAUR (10位演员) 测试通过！\n")

    film453 = film_map.get(453)
    assert film453 is not None, "测试失败: 未找到 Film ID 453"
    assert film453.title == "IMAGE PRINCESS"
    # 验证关系是列表（这是关键！修复后的代码应该保证这一点）
    assert isinstance(film453.actors, list), f"测试失败: film.actors 应该是 list, 却是 {type(film453.actors)}"
    # 验证演员数量
    assert len(film453.actors) == 11, f"测试失败: 演员数量应为 11, 却是 {len(film453.actors)}"
    assert isinstance(film453.actors[0], Actor)
    assert film453.actors[0].first_name == "ED"

    film771 = film_map.get(771)
    assert film771 is not None, "测试失败: 未找到 Film ID 771"
    assert film771.title == "SCORPION APOLLO"
    # 验证关系是列表
    assert isinstance(film771.actors, list), f"测试失败: film.actors 应该是 list, 却是 {type(film771.actors)}"
    # 验证演员数量
    assert len(film771.actors) == 8, f"测试失败: 演员数量应为 8, 却是 {len(film771.actors)}"

    print("🎉🎉🎉 所有测试用例均已通过！ 🎉🎉🎉")

def test_aggregates():
    film_ids_to_test = (1, 453, 771)
    sql_query = text(f"""
        SELECT
            f.film_id,
            f.title,
            f.description,
            COUNT(a.actor_id) AS actor_count,
            STRING_AGG(a.first_name || ' ' || a.last_name, ', ') AS actors_list,
            f.last_update AS film_last_update
        FROM
            film f
        LEFT JOIN film_actor fa ON f.film_id = fa.film_id
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
        WHERE f.film_id IN {film_ids_to_test}
        GROUP BY
            f.film_id,
            f.title,
            f.description,
            f.last_update
        ORDER BY
            f.film_id;
    """)

    with Session(engine) as session:
        df_for_dsql = pd.read_sql(sql_query, session.bind)
    

    print(df_for_dsql.head())


    # 定义参与重建的模型
    models_to_process = [Film, Actor]
    # 获取DataFrame的所有列名
    queried_columns = list(df_for_dsql.columns)

    dsql = DanticSQL(models=models_to_process, queried_columns=queried_columns)
    
    # 生成所有独立的Pydantic实例
    dsql.pydantic_all(df_for_dsql)
    
    # 连接实例之间的关系
    dsql.connect_all()



