import pandas as pd
import pytest
from sqlalchemy.orm import selectinload
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select, text

from danticsql import DanticSQL
from tests.utils import compare_pydantic_dicts
from sqlmodel.pool import StaticPool

# ----------------- SQLModel Definitions -----------------

# --- Link Tables ---
class HeroTeamLink(SQLModel, table=True):
    """Link table for the many-to-many relationship between Hero and Team."""
     

    team_id: int | None = Field(default=None, foreign_key="team.team_id", primary_key=True)
    hero_id: int | None = Field(default=None, foreign_key="hero.hero_id", primary_key=True)


# --- Main Tables ---

class City(SQLModel, table=True):
    """Represents a city where a home base can be located."""

    city_id: int | None = Field(default=None, primary_key=True)
    city_name: str = Field(index=True)
    state: str

    bases: list["HomeBase"] = Relationship(back_populates="city")


class HomeBase(SQLModel, table=True):
    """A one-to-one relationship with a Team."""
     

    base_id: int | None = Field(default=None, primary_key=True)
    base_name: str

    # Foreign key to City
    city_id: int = Field(foreign_key="city.city_id")
    city: City = Relationship(back_populates="bases")

    # This establishes the one-to-one link to Team
    team_id: int | None = Field(default=None, foreign_key="team.team_id")
    team: "Team" = Relationship(back_populates="home_base")


class Team(SQLModel, table=True):
    """A team can have many heroes."""
     

    team_id: int | None = Field(default=None, primary_key=True)
    team_name: str = Field(index=True)

    # One-to-one relationship with HomeBase
    home_base: HomeBase | None = Relationship(back_populates="team", sa_relationship_kwargs={'uselist': False})

    # Many-to-many relationship with Hero
    heroes: list["Hero"] = Relationship(back_populates="teams", link_model=HeroTeamLink)


class Power(SQLModel, table=True):
    """A hero can have multiple powers (one-to-many)."""
     

    power_id: int | None = Field(default=None, primary_key=True)
    power_name: str
    description: str

    hero_id: int | None = Field(default=None, foreign_key="hero.hero_id")
    hero: "Hero" = Relationship(back_populates="powers")


class Gadget(SQLModel, table=True):
    """A hero can have multiple gadgets (one-to-many)."""
     

    gadget_id: int | None = Field(default=None, primary_key=True)
    gadget_name: str
    functionality: str

    hero_id: int | None = Field(default=None, foreign_key="hero.hero_id")
    hero: "Hero" = Relationship(back_populates="gadgets")


class Hero(SQLModel, table=True):
    """A hero can be on many teams and have many powers/gadgets."""
     

    hero_id: int | None = Field(default=None, primary_key=True)
    hero_name: str = Field(index=True)
    secret_name: str
    age: int | None = Field(default=None, index=True)

    # One-to-many relationships
    powers: list[Power] = Relationship(back_populates="hero")
    gadgets: list[Gadget] = Relationship(back_populates="hero")

    # Many-to-many relationship
    teams: list[Team] = Relationship(back_populates="heroes", link_model=HeroTeamLink)


# ----------------- Pytest Fixture for DB Setup -----------------
@pytest.fixture(scope="function",name="session")
def db_engine_fixture():
    """
    Pytest fixture to create an in-memory SQLite database, create tables,
    and populate it with data for the test. Yields the engine session.
    """
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},poolclass=StaticPool,)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        populate_database(session)
        yield session
    SQLModel.metadata.drop_all(engine)


# ----------------- Helper Functions  -----------------
def populate_database(session:Session):
    """Create and add complex, interrelated data to the database."""
    # 1. Create Cities
    city_ny = City(city_name="New York", state="NY")
    city_la = City(city_name="Los Angeles", state="CA")

    # 2. Create Teams and their HomeBases (One-to-One)
    base_sharp_tower = HomeBase(base_name="Sharp Tower", city=city_ny)
    team_preventers = Team(team_name="Preventers", home_base=base_sharp_tower)

    base_bar = HomeBase(base_name="Sister Margaret's Bar", city=city_ny)
    team_z_force = Team(team_name="Z-Force", home_base=base_bar)

    base_west_coast = HomeBase(base_name="The Compound", city=city_la)
    team_revengers = Team(team_name="The Revengers", home_base=base_west_coast)

    # 3. Create Heroes with their Powers and Gadgets (One-to-Many)
    hero_rusty_man = Hero(
        hero_name="Rusty-Man",
        secret_name="Tommy Sharp",
        age=48,
        powers=[
            Power(power_name="Genius Intellect", description="Can build anything."),
            Power(power_name="Powered Armor", description="Grants flight and strength.")
        ],
        gadgets=[
            Gadget(gadget_name="Repulsor Rays", functionality="Energy blasts."),
            Gadget(gadget_name="Arc Reactor", functionality="Energy source.")
        ],
        teams=[team_preventers, team_revengers] # On two teams
    )

    hero_spider_boy = Hero(
        hero_name="Spider-Boy",
        secret_name="Pedro Parqueador",
        age=19,
        powers=[
            Power(power_name="Wall-Crawling", description="Sticks to surfaces."),
            Power(power_name="Spider-Sense", description="Precognitive danger sense.")
        ],
        gadgets=[
            Gadget(gadget_name="Web-Shooters", functionality="Shoots synthetic webbing.")
        ],
        teams=[team_preventers] # On one team
    )

    hero_deadpond = Hero(
        hero_name="Deadpond",
        secret_name="Dive Wilson",
        age=32,
        powers=[
            Power(power_name="Regenerative Healing Factor", description="Heals from any injury."),
        ],
        gadgets=[
            Gadget(gadget_name="Katanas", functionality="Very sharp."),
            Gadget(gadget_name="Teleportation Belt", functionality="Unreliable short-range teleport.")
        ],
        teams=[team_z_force, team_revengers] # On two different teams
    )

    hero_captain_canada = Hero(
        hero_name="Captain Canada",
        secret_name="Steve Deers",
        age=93,
        powers=[Power(power_name="Super-Soldier Serum", description="Peak human abilities.")],
        gadgets=[Gadget(gadget_name="Vibranium-Alloy Shield", functionality="Indestructible shield.")],
        teams=[team_preventers] # Just on one team
    )

    # Add all objects to the session
    session.add(hero_rusty_man)
    session.add(hero_spider_boy)
    session.add(hero_deadpond)
    session.add(hero_captain_canada)

    session.commit()

    # Refresh to load all the new relationships
    for hero in [hero_rusty_man, hero_spider_boy, hero_deadpond, hero_captain_canada]:
        session.refresh(hero)
    for team in [team_preventers, team_z_force, team_revengers]:
         session.refresh(team)

def select_all_orm(session:Session):
    """Select all teams and heroes with their relationships."""
    citys_query = select(City).options(selectinload(City.bases).selectinload(HomeBase.team))
    cities = session.exec(citys_query).all()

    teams_query = select(Team).options(selectinload(Team.heroes), selectinload(Team.home_base).selectinload(HomeBase.city))
    teams = session.exec(teams_query).all()

    heroes_query = select(Hero).options(selectinload(Hero.teams), selectinload(Hero.powers), selectinload(Hero.gadgets))
    heroes = session.exec(heroes_query).all()

    return {"city": cities, "team": teams, "hero": heroes}

def select_all_sql(session:Session) -> pd.DataFrame:
    """Select all teams and heroes with their relationships using raw SQL."""
    sql = """SELECT
    h.hero_id,
    h.hero_name,
    h.secret_name,
    h.age,
    p.power_id,
    p.power_name,
    p.description AS power_description,
    g.gadget_id,
    g.gadget_name,
    g.functionality,
    t.team_id,
    t.team_name,
    hb.base_id,
    hb.base_name,
    c.city_id,
    c.city_name,
    c.state
FROM
    hero AS h
LEFT JOIN
    power AS p ON h.hero_id = p.hero_id
LEFT JOIN
    gadget AS g ON h.hero_id = g.hero_id
LEFT JOIN
    heroteamlink AS htl ON h.hero_id = htl.hero_id
LEFT JOIN
    team AS t ON htl.team_id = t.team_id
LEFT JOIN
    homebase AS hb ON t.team_id = hb.team_id
LEFT JOIN
    city AS c ON hb.city_id = c.city_id;
    """
    statement = text(sql)
    result = session.exec(statement)
    dict_results = result.mappings().all()
    df = pd.DataFrame(dict_results, dtype=object)
    return df

def get_danticsql_results(session:Session):
    df = select_all_sql(session)
    dan = DanticSQL([Team, Hero, City  ], ["age","base_id","base_name","city_id","city_name",
                     "gadget_id","gadget_name","functionality","hero_id","hero_name",
                     "power_id","power_name","power_description","secret_name","team_id",
                     "team_name","state"])
    dan.pydantic_all(df)
    dan.connect_all()
    return dan.instances

def test_complex_reconstruction(session:Session):
    """
    Tests that DanticSQL can correctly reconstruct complex relationships
    from a flat DataFrame, matching the result from a direct ORM query.
    """
    # 1. Get results from the library being tested (DanticSQL)
    dantic_results = get_danticsql_results(session)

    # 2. Get the ground truth / expected results directly from the ORM
    orm_results = select_all_orm(session)

    # 3. Assert that the results are identical
    assert compare_pydantic_dicts(dantic_results, orm_results)
