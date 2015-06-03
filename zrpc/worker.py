
import zmq


class Worker(object):
    def __init__(self):
        self.fn = {}

    def append(self, name, callback):
        self.fn[name.encode('utf8')] = callback

    def serve(self, host):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(host)

        keys = b','.join(sorted(self.fn.keys()))
        socket.send_multipart([b'rg', keys])

        while True:
            raw = socket.recv_multipart()
            if raw[0] == b'$ping':
                print('ping')
                socket.send_multipart([b'pr'])
                continue

            name, data = raw
            result = self.fn[name](data)
            socket.send_multipart([b'rs', result])
