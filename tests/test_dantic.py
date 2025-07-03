from danticsql import DanticSQL
import pandas as pd
from .example_model import Gene,Study,Probe,Trait

df = pd.read_csv("tests/data/data.csv")

dan = DanticSQL([Gene,Study,Probe,Trait],["gene_name","gene_id","probe_id","trait_name","study_id"])
dan.pydantic_all(df)
dan.connect_all()
