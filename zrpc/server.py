
import zmq
from collections import deque, defaultdict
import time

WORKER_TIMEOUT = 60  # ping worker before send job if it has not had any job for 60 sec


def serve(host="tcp://127.0.0.1:5000"):
    context = zmq.Context()
    socket = context.socket(zmq.XREP)
    socket.bind(host)

    group_by_fn = {}
    group_by_worker = {}
    workers_by_group = defaultdict(deque)
    tasks_by_group = defaultdict(deque)
    pair = {}

    workers_time = {}  # last time when worker was active

    #log = print
    log = lambda *argv, **kargs: None

    def run_task(worker_addr, client_addr, name, data):
        pair[worker_addr] = client_addr
        log('send worker', worker_addr, name, data)
        socket.send_multipart([worker_addr, b'', name, data])  # fn + arg

    while True:
        log('recv')
        # addr/client, b'', cmd, fn, args
        raw = socket.recv_multipart()
        log(raw)
        cmd = raw[2]

        if cmd == b'do':
            client_addr = raw[0]
            name = raw[3]
            data = raw[4]
            try:
                group_id = group_by_fn[name]
            except KeyError:
                socket.send_multipart([client_addr, b'', b'', b'nm'])  # no method
                continue
            wgroup = workers_by_group[group_id]
            while True:
                if wgroup:
                    worker_addr = wgroup.popleft()
                    # ping worker if it is not active
                    if workers_time[worker_addr] + WORKER_TIMEOUT < time.time():
                        print('ping')
                        pair[worker_addr] = None
                        socket.send_multipart([worker_addr, b'', b'$ping'])
                        continue  # try take next one worker
                    else:
                        run_task(worker_addr, client_addr, name, data)
                else:
                    tasks_by_group[group_id].append((client_addr, name, data))
                break
        elif cmd == b'rs':  # response
            worker_addr = raw[0]
            result = raw[3]
            log('worker', worker_addr)

            client_addr = pair[worker_addr]
            log('send client', client_addr)
            socket.send_multipart([client_addr, b'', result])
            pair[worker_addr] = None

            group_id = group_by_worker[worker_addr]
            tasks = tasks_by_group[group_id]
            if tasks:
                task = tasks.popleft()
                run_task(worker_addr, task[0], task[1], task[2])
            else:
                workers_by_group[group_id].append(worker_addr)
            workers_time[worker_addr] = time.time()
        elif cmd == b'rg':
            worker_addr = raw[0]
            fn_list = raw[3]
            group_id = hash(fn_list)
            print('worker', worker_addr, fn_list, group_id)

            group_by_worker[worker_addr] = group_id
            workers_by_group[group_id].append(worker_addr)
            workers_time[worker_addr] = time.time()
            for name in fn_list.split(b','):
                if name in group_by_fn:
                    if group_by_fn[name] != group_id:
                        log('Warning: function collision')
                group_by_fn[name] = group_id
        elif cmd == b'pr':  # ping response
            worker_addr = raw[0]
            group_id = group_by_worker[worker_addr]
            tasks = tasks_by_group[group_id]
            if tasks:
                task = tasks.popleft()
                run_task(worker_addr, task[0], task[1], task[2])
            else:
                workers_by_group[group_id].append(worker_addr)
            workers_time[worker_addr] = time.time()
        else:
            raise Exception('Wrong command')
