#%%
import zmq
from pathlib import Path


ctx = zmq.Context()
socket = ctx.socket(zmq.PAIR)
port = socket.bind_to_random_port("tcp://127.0.0.1", min_port=55555, max_port=55565)
print(port)
socket.close()

socket = ctx.socket(zmq.PAIR)
socket.bind("tcp://127.0.0.1:55560")
socket.close()


port
#%%
globals()

#%%
#!%load_ext dabbler.ext
#%%
ip = get_ipython()
ip.ev("dir()")['__file__']

ip.ev('__file__')

#%%
import zmq
from pathlib import Path

ctx1 = zmq.Context()
ctx2 = zmq.Context()

s1 = ctx1.socket(zmq.PAIR)
c1= ctx2.socket(zmq.PAIR)
#%%

# s1.bind_to_random_port("tcp://127.0.0.1", min_port=55555, max_port=55565)
s1.bind("tcp://127.0.0.1:55564")
c1.connect("tcp://127.0.0.1:55564")
# %%
c1.send(b'hello')
s1.send(b'hello')
s1.poll(100)
#%%
s1.recv()

# %%
s2.send(b'hello',zmq.NOBLOCK)

#%%
s1.send(b'hello')

#%%
s1.poll(100)
s1.recv()


#%%
s2.poll(100)
s2.recv(zmq.NOBLOCK)
#%%
import os
# for os.environ


#%%

import tempfile
tempfile.gettempdir()

#%%

try:
    s3.bind("tcp://127.0.0.1:55564")
except:
    s3.connect("tcp://127.0.0.1:55564")
    


# %%
s3.send(b'hello')

#%%
s3.poll(100)

#%%
p = Path(__file__)
d = p.parent
d2 = d.parent
# %%
p.parents.index()

#%%
import os
os.getlogin()
os.environ.get('VIRTUAL_ENV', None) == 'C:\\Projects\\db_dabbler\\db_dabbler_env'