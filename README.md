Example of RPC broker over zeromq

**client.py**
```python
from zrpc.client import Client

client = Client()
client.open("tcp://127.0.0.1:5000")

response = client.call_raw('echo', b'ping')

print(response)
client.close()
```

**worker.py**
```python
from zrpc.worker import Worker

def echo(data):
    return data

worker = Worker()
worker.append('echo', echo)
worker.serve("tcp://127.0.0.1:5000")
```

**server.py**
```python
from zrpc.server import serve

serve("tcp://127.0.0.1:5000")
```
