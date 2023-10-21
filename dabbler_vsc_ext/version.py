#%%
from pathlib import Path
import re

pkg = Path(__file__).parent / 'package.json'

txt = pkg.read_text()

pat = re.compile(r'"version":\s*"(\d+\.\d+\.\d+)"')

m = pat.search(txt)
print(f'prior version: {m.group(1)}')

#print current version and increment last part of version 0.0.x to 0.0.x+1
txt = pat.sub(lambda m: f'"version": "{m.group(1)[:-1]}{int(m.group(1)[-1])+1}"', txt)

m = pat.search(txt)
print(f'new version:   {m.group(1)}')

pkg.write_text(txt)