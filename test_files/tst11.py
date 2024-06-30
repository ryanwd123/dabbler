#%%
# 
from pathlib import Path
from tkinter import N
import polars as pl
from qtpy import QtWidgets, QtCore, QtGui
from dabbler.gui_table import DfView
from dabbler.gui_stuff import gui_style
import duckdb
db = duckdb.connect()



def get_imdb_df():
    f = Path(__file__).parent
    imdb = f/'../../sample_data/imdb.db'
    db.execute(f"attach '{imdb}' as imdb (read_only true)")

    df = db.sql(
    """--sql,
    select
        *
    from imdb.main.titles
    limit 2000000
    --limit 200
    """
    ).pl()
    return df

df = get_imdb_df()
# %%
masks = {}
for i in range(20000):
    masks[i] = df['runtime_minutes'] > i
# %%

(5698 - 722)/20000
i = None
df.estimated_size()
df.shape