#%%
from qtpy.QtWidgets import QApplication, QLabel
from qtpy.QtGui import QFont, QFontDatabase, QFontMetrics
import polars as pl



app=QApplication([])
#%%
font = QFont()
fonts = ['Calibri', 'Arial', 'Helvetica', 'sans-serif']
font.setFamilies(fonts)
font.setPointSize(10)
fm = QFontMetrics(font)

fm.horizontalAdvance('0'*100)
# %%
df = pl.DataFrame({'font':fonts})
# %%
df['font'].str.len_chars().median()