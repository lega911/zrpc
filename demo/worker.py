
from zrpc.worker import Worker


def echo(data):
    return data

worker = Worker()
worker.append('echo', echo)
worker.serve("tcp://127.0.0.1:5000")
