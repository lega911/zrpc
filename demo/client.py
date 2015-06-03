
from zrpc.client import Client

client = Client()
client.open("tcp://127.0.0.1:5000")

response = client.call_raw('echo', b'ping')

print(response)
client.close()
