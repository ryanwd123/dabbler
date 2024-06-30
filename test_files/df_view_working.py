#%%
from pathlib import Path
from tkinter import N
import polars as pl
from qtpy import QtWidgets, QtCore, QtGui
from dabbler.gui_table import DfView
from dabbler.gui_stuff import gui_style
import duckdb
from functools import reduce



def get_imdb_df():
    db = duckdb.connect()   
    f = Path(__file__).parent
    imdb = f/'../../sample_data/imdb.db'
    db.execute(f"attach '{imdb}' as imdb (read_only true)")

    df = db.sql(
    """--sql,
    select
        *
    from imdb.main.titles
    limit 200000
    --limit 200
    """
    ).pl()
    db.close()
    return df
#%%
# q = df['primary_title'].str.len_chars().quantile(0.9)
# c = df['primary_title'].filter(df['primary_title'].str.len_chars() < q)
# top = c.str.len_chars().arg_sort(descending=True).to_list()[:10]


# [print(c[val],len(c[val])) for val in top]



#%%
def get_duck_db_columns_df():
    db= duckdb.connect()
    df = db.sql(
    """--sql,
    select
        * REPLACE (tags::VARCHAR AS tags)
    from duckdb_types
    """
    ).pl()
    return df

# df = get_duck_db_columns_df()
#%%
df = get_imdb_df()
#%%
# with pl.StringCache():
#     df = df.with_columns(
#         pl.col(pl.String).cast(pl.Categorical)
#     )

#%%
# df['genres'].is_in(df['genres'].cat.get_categories().filter(df['genres'].cat.get_categories().str.contains('Short')))

pl.col(pl.Categorical).is_in(pl.col(pl.Categorical).cat.get_categories().filter(pl.col(pl.Categorical).cat.get_categories().str.contains('Short')))

#%%

non_catigoral = [c for c,dt in df.schema.items() if dt != pl.Categorical]
catagorical = [c for c,dt in df.schema.items() if dt == pl.Categorical]

filters =     [pl.any_horizontal(pl.col(non_catigoral).cast(pl.String).str.contains('1912')),
    pl.any_horizontal(pl.col(catagorical).is_in(pl.col(catagorical).cat.get_categories().filter(pl.col(catagorical).cat.get_categories().str.contains(rf"(?i)ad")))),
    pl.all_horizontal(~pl.col(non_catigoral).fill_null('').str.contains('short')),
    pl.all_horizontal(~pl.col(catagorical).is_in(pl.col(catagorical).cat.get_categories().filter(pl.col(catagorical).cat.get_categories().str.contains('short')))),]

for f in filters:
    print(f)
df.filter(
    filters
)

#%%
# Example DataFrame
df2 = pl.DataFrame({
    'A': ['apple pie', 'banana', 'grape juice', 'apple grape', 'orange', 'j'],
    'B': ['pear', 'apple', 'kiwi', 'grape', 'apple cider',' k'],
    'C': ['fruit salad', 'apple grape', 'orange', 'pineapple', 'grapefruit', 'm']
})
df2
# Correct way to filter the DataFrame
filtered_df = df2.filter(
    [pl.any_horizontal(pl.col('*').str.contains('apple')),
    pl.all_horizontal(~pl.col('*').str.contains('grape'))]
)

print(filtered_df)
# df['parameters'].list.join(', ').cast(pl.String)
# %%

app = QtWidgets.QApplication([])

w = DfView(parent=None,app=app)
w.set_df(df)

# fonts = QtGui.QFontDatabase().families()
# print(fonts)

w.show()
w.setGeometry(-2000,100,1920,800)
app.exec_()
# %%
