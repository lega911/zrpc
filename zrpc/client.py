
import zmq


class Client(object):
    def open(self, host):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.REQ)
        self.socket.connect(host)

    def close(self):
        self.socket.close()
        self.ctx.term()

    def call_raw(self, name, data):
        self.socket.send_multipart([b'do', name.encode('utf8'), data])
        r = self.socket.recv_multipart()
        if len(r) == 2 and r[1] == b'nm':
            raise Exception('No method')
        return r[0]
