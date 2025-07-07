import pandas as pd
import pytest
from sqlalchemy.orm import selectinload
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select, text
from sqlmodel.pool import StaticPool

from danticsql import DanticSQL
from tests.utils import compare_pydantic_dicts

from sqlalchemy import MetaData

class BaseSQLModel(SQLModel):
    metadata = MetaData()

# ----------------- SQLModel Definitions -----------------
class TeamOneToMany(BaseSQLModel, table=True ):
    __tablename__ = "team"

    team_id: int | None = Field(default=None, primary_key=True)
    team_name: str = Field(index=True)
    headquarters: str
    heroes: list["HeroOneToMany"] = Relationship(back_populates="team")

class HeroOneToMany(BaseSQLModel, table=True):
    __tablename__ = "hero"

    hero_id: int | None = Field(default=None, primary_key=True)
    hero_name: str = Field(index=True)
    secret_name: str
    age: int | None = Field(default=None, index=True)
    team_id: int | None = Field(default=None, foreign_key="team.team_id")
    team: TeamOneToMany | None = Relationship(back_populates="heroes")


# ----------------- Pytest Fixture for DB Setup -----------------
@pytest.fixture(scope="function",name="session")
def db_engine_fixture():
    """
    Pytest fixture to create an in-memory SQLite database, create tables,
    and populate it with data for the test. Yields the engine session.
    """
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},poolclass=StaticPool,)
    BaseSQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        populate_database(session)
        yield session
    BaseSQLModel.metadata.drop_all(engine)


# ----------------- Helper Functions  -----------------
def populate_database(session:Session):
    team_preventers = TeamOneToMany(team_name="Preventers", headquarters="Sharp Tower")
    team_z_force = TeamOneToMany(team_name="Z-Force", headquarters="Sister Margaret's Bar")
    hero_deadpond = HeroOneToMany(hero_name="Deadpond", secret_name="Dive Wilson", team=team_z_force)
    hero_rusty_man = HeroOneToMany(hero_name="Rusty-Man", secret_name="Tommy Sharp", age=48, team=team_preventers)
    hero_spider_boy = HeroOneToMany(hero_name="Spider-Boy", secret_name="Pedro Parqueador")
    session.add(hero_deadpond)
    session.add(hero_rusty_man)
    session.add(hero_spider_boy)
    session.commit()
    session.refresh(hero_deadpond)
    session.refresh(hero_rusty_man)
    session.refresh(hero_spider_boy)
    hero_spider_boy.team = team_preventers
    session.add(hero_spider_boy)
    session.commit()
    session.refresh(hero_spider_boy)
    hero_black_lion = HeroOneToMany(hero_name="Black Lion", secret_name="Trevor Challa", age=35)
    hero_sure_e = HeroOneToMany(hero_name="Princess Sure-E", secret_name="Sure-E")
    team_wakaland = TeamOneToMany(
        team_name="Wakaland",
        headquarters="Wakaland Capital City",
        heroes=[hero_black_lion, hero_sure_e],
    )
    session.add(team_wakaland)
    session.commit()
    session.refresh(team_wakaland)
    hero_tarantula = HeroOneToMany(hero_name="Tarantula", secret_name="Natalia Roman-on", age=32)
    hero_dr_weird = HeroOneToMany(hero_name="Dr. Weird", secret_name="Steve Weird", age=36)
    hero_cap = HeroOneToMany(hero_name="Captain North America", secret_name="Esteban Rogelios", age=93)
    team_preventers.heroes.append(hero_tarantula)
    team_preventers.heroes.append(hero_dr_weird)
    team_preventers.heroes.append(hero_cap)
    session.add(team_preventers)
    session.commit()

def select_all_orm(session:Session):
    teams_query = select(TeamOneToMany).options(selectinload(TeamOneToMany.heroes))
    teams = session.exec(teams_query).all()
    heroes_query = select(HeroOneToMany).options(selectinload(HeroOneToMany.team))
    heroes = session.exec(heroes_query).all()
    return {"team": teams, "hero": heroes}

def select_all_sql(session:Session) -> pd.DataFrame:
    sql = """
    SELECT h.hero_id, h.hero_name, h.secret_name, h.age, t.team_id, t.team_name, t.headquarters
    FROM hero AS h LEFT JOIN team AS t ON h.team_id = t.team_id;
    """
    result = session.exec(text(sql)).mappings().all()
    return pd.DataFrame(result, dtype=object)

def get_danticsql_results(session:Session):
    df = select_all_sql(session)
    dan = DanticSQL([TeamOneToMany, HeroOneToMany], ["age", "headquarters", "hero_id", "hero_name", "secret_name", "team_id", "team_name"])
    dan.pydantic_all(df)
    dan.connect_all()
    return dan.instances

# ----------------- The Pytest Test Function -----------------
def test_one_to_many_reconstruction(session:Session):
    """
    Tests that DanticSQL can correctly reconstruct a one-to-many relationship
    from a flat DataFrame, matching the result from a direct ORM query.
    """
    # 1. Get results from the library being tested (DanticSQL)
    dantic_results = get_danticsql_results(session)

    # 2. Get the ground truth / expected results directly from the ORM
    orm_results = select_all_orm(session)

    # 3. Assert that the results are identical
    assert compare_pydantic_dicts(dantic_results, orm_results)
