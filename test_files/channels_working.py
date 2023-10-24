#%%
import zmq
from pathlib import Path

ctx1 = zmq.Context()
ctx2 = zmq.Context()
ctx3 = zmq.Context()

s1 = ctx1.socket(zmq.PUSH)
s2 = ctx2.socket(zmq.PULL)
s3 = ctx3.socket(zmq.PULL)


# s1.bind_to_random_port("tcp://127.0.0.1", min_port=55555, max_port=55565)
s1.bind("tcp://127.0.0.1:55564")
# %%



s2.connect("tcp://127.0.0.1:55564")
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