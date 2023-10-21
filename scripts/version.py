#%%
from pathlib import Path
import re

dist = Path(__file__).parent.parent / 'dist'
for f in dist.glob('*'):
    print(f'delete {f}')
    f.unlink()



file = Path(__file__).parent.parent / 'dabbler' / '__init__.py'


txt = file.read_text()
pat = r'(?P<name>__version__:\s+str\s*=\s*)"(?P<version>(?P<v1>\d+)[.](?P<v2>\d+)[.](?P<v3>\d+))"'
m = re.match(pat,txt)
print(f'prior version:  {m.groupdict()["version"]}')
v = int(m.groupdict()['v3']) + 1
txt2 = re.sub(r'(?P<name>__version__:\s+str\s*=\s*)"(?P<v1>\d+)[.](?P<v2>\d+)[.](?P<v3>\d+)"',f'\g<name>"\g<v1>.\g<v2>.{v}"',txt)
m = re.match(pat,txt2)
print(f'new version:    {m.groupdict()["version"]}')
file.write_text(txt2)
# %%
