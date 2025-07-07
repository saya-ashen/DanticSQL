from typing import cast
import pandas as pd
from sqlalchemy.orm import selectinload
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select, text

from danticsql import DanticSQL
from tests.utils import compare_pydantic_dicts


class HeroTeamLink(SQLModel, table=True):
    team_id: int | None = Field(default=None, foreign_key="team.team_id", primary_key=True)
    hero_id: int | None = Field(default=None, foreign_key="hero.hero_id", primary_key=True)


class Team(SQLModel, table=True):
    team_id: int | None = Field(default=None, primary_key=True)
    team_name: str = Field(index=True)
    headquarters: str

    heroes: list["Hero"] = Relationship(back_populates="teams", link_model=HeroTeamLink)


class Hero(SQLModel, table=True):
    hero_id: int | None = Field(default=None, primary_key=True)
    hero_name: str = Field(index=True)
    secret_name: str
    age: int | None = Field(default=None, index=True)

    teams: list[Team] = Relationship(back_populates="heroes", link_model=HeroTeamLink)

sqlite_url = f"sqlite:///:memory:"

engine = create_engine(sqlite_url, echo=True, connect_args={"check_same_thread": False})


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def create_heroes():
    with Session(engine) as session:
        team_preventers = Team(team_name="Preventers", headquarters="Sharp Tower")
        team_z_force = Team(team_name="Z-Force", headquarters="Sister Margaret's Bar")

        hero_deadpond = Hero(
            hero_name="Deadpond",
            secret_name="Dive Wilson",
            teams=[team_z_force, team_preventers],
        )
        hero_rusty_man = Hero(
            hero_name="Rusty-Man",
            secret_name="Tommy Sharp",
            age=48,
            teams=[team_preventers],
        )
        hero_spider_boy = Hero(
            hero_name="Spider-Boy", secret_name="Pedro Parqueador", teams=[team_preventers]
        )
        session.add(hero_deadpond)
        session.add(hero_rusty_man)
        session.add(hero_spider_boy)
        session.commit()

        session.refresh(hero_deadpond)
        session.refresh(hero_rusty_man)
        session.refresh(hero_spider_boy)

        print("Deadpond:", hero_deadpond)
        print("Deadpond teams:", hero_deadpond.teams)
        print("Rusty-Man:", hero_rusty_man)
        print("Rusty-Man Teams:", hero_rusty_man.teams)
        print("Spider-Boy:", hero_spider_boy)
        print("Spider-Boy Teams:", hero_spider_boy.teams)

def select_all_teams_and_heroes()->dict:
    with Session(engine) as session:
        teams_query = select(Team).options(selectinload(Team.heroes)) # type: ignore
        teams = session.exec(teams_query).all()
        heroes_query = select(Hero).options(selectinload(Hero.teams)) # type: ignore

        heroes = session.exec(heroes_query).all()

    return {"team": teams, "hero": heroes}


def select_all_teams_and_heroes_sql()->pd.DataFrame:
    sql = """
    SELECT
    h.hero_id,
    h.hero_name,
    h.secret_name,
    h.age,
    t.team_id,
    t.team_name,
    t.headquarters
FROM
  hero AS h
LEFT JOIN
  heroteamlink AS htl ON h.hero_id= htl.hero_id
LEFT JOIN
  team AS t ON htl.team_id = t.team_id;"""
    with Session(engine) as session:
        statement = text(sql)
        result = session.exec(statement) # type: ignore
        dict_results = result.mappings().all()
        for record in dict_results:
            print(record)
        df = pd.DataFrame(dict_results,dtype=object)
    return df

def main():
    create_db_and_tables()
    create_heroes()
    df = select_all_teams_and_heroes_sql()
    dan = DanticSQL([Team,Hero],[
        "age","headquarters","hero_id","hero_name","secret_name","team_id","team_name"])
    dan.pydantic_all(df)
    dan.connect_all()

    # 对比两种方法获取的结果是否相同
    r1 = dan.instances
    r2 = select_all_teams_and_heroes()
    assert compare_pydantic_dicts(r1,r2)

if __name__ == "__main__":
    main()
