from typing import cast
import pandas as pd
from sqlalchemy.orm import selectinload
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select, text

from danticsql import DanticSQL


class Team(SQLModel, table=True):
    team_id: int | None = Field(default=None, primary_key=True)
    team_name: str = Field(index=True)
    headquarters: str

    heroes: list["Hero"] = Relationship(back_populates="team")


class Hero(SQLModel, table=True):
    hero_id: int | None = Field(default=None, primary_key=True)
    hero_name: str = Field(index=True)
    secret_name: str
    age: int | None = Field(default=None, index=True)

    team_id: int | None = Field(default=None, foreign_key="team.team_id")
    team: Team | None = Relationship(back_populates="heroes")


sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

engine = create_engine(sqlite_url, echo=True)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def create_heroes():
    with Session(engine) as session:
        team_preventers = Team(team_name="Preventers", headquarters="Sharp Tower")
        team_z_force = Team(team_name="Z-Force", headquarters="Sister Margaret's Bar")

        hero_deadpond = Hero(
            hero_name="Deadpond", secret_name="Dive Wilson", team=team_z_force
        )
        hero_rusty_man = Hero(
            hero_name="Rusty-Man", secret_name="Tommy Sharp", age=48, team=team_preventers
        )
        hero_spider_boy = Hero(hero_name="Spider-Boy", secret_name="Pedro Parqueador")
        session.add(hero_deadpond)
        session.add(hero_rusty_man)
        session.add(hero_spider_boy)
        session.commit()

        session.refresh(hero_deadpond)
        session.refresh(hero_rusty_man)
        session.refresh(hero_spider_boy)

        print("Created hero:", hero_deadpond)
        print("Created hero:", hero_rusty_man)
        print("Created hero:", hero_spider_boy)

        hero_spider_boy.team = team_preventers
        session.add(hero_spider_boy)
        session.commit()
        session.refresh(hero_spider_boy)
        print("Updated hero:", hero_spider_boy)

        hero_black_lion = Hero(hero_name="Black Lion", secret_name="Trevor Challa", age=35)
        hero_sure_e = Hero(hero_name="Princess Sure-E", secret_name="Sure-E")
        team_wakaland = Team(
            team_name="Wakaland",
            headquarters="Wakaland Capital City",
            heroes=[hero_black_lion, hero_sure_e],
        )
        session.add(team_wakaland)
        session.commit()
        session.refresh(team_wakaland)
        print("Team Wakaland:", team_wakaland)

        hero_tarantula = Hero(hero_name="Tarantula", secret_name="Natalia Roman-on", age=32)
        hero_dr_weird = Hero(hero_name="Dr. Weird", secret_name="Steve Weird", age=36)
        hero_cap = Hero(
            hero_name="Captain North America", secret_name="Esteban Rogelios", age=93
        )

        team_preventers.heroes.append(hero_tarantula)
        team_preventers.heroes.append(hero_dr_weird)
        team_preventers.heroes.append(hero_cap)
        session.add(team_preventers)
        session.commit()
        session.refresh(hero_tarantula)
        session.refresh(hero_dr_weird)
        session.refresh(hero_cap)
        print("Preventers new hero:", hero_tarantula)
        print("Preventers new hero:", hero_dr_weird)
        print("Preventers new hero:", hero_cap)




def select_all_teams_and_heroes()->dict:
    with Session(engine) as session:
        teams_query = select(Team).options(selectinload(Team.heroes)) # type: ignore
        teams = session.exec(teams_query).all()
        heroes_query = select(Hero).options(selectinload(Hero.team)) # type: ignore

        heroes = session.exec(heroes_query).all()

    return {"team": teams, "hero": heroes}


def select_all_teams_and_heroes_sql()->pd.DataFrame:
    sql = """SELECT
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
    team AS t ON h.team_id = t.team_id;"""
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
    teams1 = cast(list[Team],r1["team"])
    teams2 = cast(list[Team],r2["team"])
    for team1, team2 in zip(teams1, teams2):
        assert team1.team_id == team2.team_id
        assert team1.team_name == team2.team_name
        assert team1.headquarters == team2.headquarters
        for hero1, hero2 in zip(team1.heroes, team2.heroes):
            assert hero1.hero_id == hero2.hero_id
            assert hero1.hero_name == hero2.hero_name
            assert hero1.secret_name == hero2.secret_name
            assert hero1.age == hero2.age, f"Expected {hero1.age} but got {hero2.age}"

    heroes1 = cast(list[Hero], r1["hero"])
    heroes2 = cast(list[Hero], r2["hero"])
    for hero1, hero2 in zip(heroes1, heroes2):
        assert hero1.hero_id == hero2.hero_id
        assert hero1.hero_name == hero2.hero_name
        assert hero1.secret_name == hero2.secret_name
        assert hero1.age == hero2.age, f"Expected {hero1.age} but got {hero2.age}"
        if hero1.team:
            assert hero1.team.team_id == hero2.team.team_id
            assert hero1.team.team_name == hero2.team.team_name
            assert hero1.team.headquarters == hero2.team.headquarters
        else:
            assert hero2.team is None

    print("All tests passed successfully!")


if __name__ == "__main__":
    main()
