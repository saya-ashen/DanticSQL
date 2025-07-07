import pytest
import pandas as pd
from sqlalchemy.orm import selectinload
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select, text

from danticsql import DanticSQL
from tests.utils import compare_pydantic_dicts
from sqlmodel.pool import StaticPool

from sqlalchemy import MetaData

class BaseSQLModel(SQLModel):
    metadata = MetaData()

# ----------------- SQLModel Definitions -----------------
class HeroTeamLinkManyToMany(BaseSQLModel, table=True,):
    __tablename__ = "heroteamlink"

    team_id: int | None = Field(default=None, foreign_key="team.team_id", primary_key=True)
    hero_id: int | None = Field(default=None, foreign_key="hero.hero_id", primary_key=True)

class TeamManyToMany(BaseSQLModel, table=True,):
    __tablename__ = "team"

    team_id: int | None = Field(default=None, primary_key=True)
    team_name: str = Field(index=True)
    headquarters: str
    heroes: list["HeroManyToMany"] = Relationship(back_populates="teams", link_model=HeroTeamLinkManyToMany)

class HeroManyToMany(BaseSQLModel, table=True,):
    __tablename__ = "hero"

    hero_id: int | None = Field(default=None, primary_key=True)
    hero_name: str = Field(index=True)
    secret_name: str
    age: int | None = Field(default=None, index=True)
    teams: list[TeamManyToMany] = Relationship(back_populates="heroes", link_model=HeroTeamLinkManyToMany)

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
    team_preventers = TeamManyToMany(team_name="Preventers", headquarters="Sharp Tower")
    team_z_force = TeamManyToMany(team_name="Z-Force", headquarters="Sister Margaret's Bar")
    hero_deadpond = HeroManyToMany(hero_name="Deadpond", secret_name="Dive Wilson", teams=[team_z_force, team_preventers])
    hero_rusty_man = HeroManyToMany(hero_name="Rusty-Man", secret_name="Tommy Sharp", age=48, teams=[team_preventers])
    hero_spider_boy = HeroManyToMany(hero_name="Spider-Boy", secret_name="Pedro Parqueador", teams=[team_preventers])
    session.add(hero_deadpond)
    session.add(hero_rusty_man)
    session.add(hero_spider_boy)
    session.commit()


def select_all_orm(session:Session):
    teams_query = select(TeamManyToMany).options(selectinload(TeamManyToMany.heroes))
    teams = session.exec(teams_query).all()
    heroes_query = select(HeroManyToMany).options(selectinload(HeroManyToMany.teams))
    heroes = session.exec(heroes_query).all()
    return {"team": teams, "hero": heroes}

def select_all_sql(session:Session) -> pd.DataFrame:
    sql = """
    SELECT h.hero_id, h.hero_name, h.secret_name, h.age, t.team_id, t.team_name, t.headquarters
    FROM hero AS h
    LEFT JOIN heroteamlink AS htl ON h.hero_id = htl.hero_id
    LEFT JOIN team AS t ON htl.team_id = t.team_id;
    """
    result = session.exec(text(sql)).mappings().all()
    return pd.DataFrame(result, dtype=object)

def get_danticsql_results(session:Session):
    df = select_all_sql(session)
    dan = DanticSQL([TeamManyToMany, HeroManyToMany], ["age", "headquarters", "hero_id", "hero_name", "secret_name", "team_id", "team_name"])
    dan.pydantic_all(df)
    dan.connect_all()
    return dan.instances

# ----------------- The Pytest Test Function -----------------
def test_many_to_many_reconstruction(session:Session):
    """
    Tests that DanticSQL can correctly reconstruct a many-to-many relationship
    from a flat DataFrame, matching the result from a direct ORM query.
    """
    # 1. Get results from the library being tested (DanticSQL)
    dantic_results = get_danticsql_results(session)
    
    # 2. Get the ground truth / expected results directly from the ORM
    orm_results = select_all_orm(session)
    
    # 3. Assert that the results are identical
    assert compare_pydantic_dicts(dantic_results, orm_results)
