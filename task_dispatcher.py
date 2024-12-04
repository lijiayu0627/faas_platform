import argparse
import queue
import time
from collections import OrderedDict
from multiprocessing import Pool, Queue, Process, Manager
import zmq
from serialize_deserialize import serialize, deserialize
from execute_task import execute_task
import redis_client
from constants import *


def report_result(tup):
    task_id, status, result = tup
    redis_client.update_task(task_id, status=status, result=result)


def dispatch_local(worker_num):
    with Pool(processes=worker_num) as pool:
        pubsub = redis_client.my_redis.pubsub()
        pubsub.subscribe('tasks')
        print('Subscribed to task channel. Waiting for messages...')
        try:
            for message in pubsub.listen():
                if message['type'] == 'message':
                    task_id = message['data']
                    task = redis_client.get_task(task_id)
                    ser_fn = redis_client.get_function(task['function_id'])
                    ser_params = task['payload']
                    redis_client.update_task(task_id, status=TASK_RUNNING)
                    pool.apply_async(execute_task, (task_id, ser_fn, ser_params), callback=report_result)
        except KeyboardInterrupt:
            print('Shutting down...')
            pool.terminate()
            pool.join()
        finally:
            pubsub.unsubscribe()
            pubsub.close()


def subscribe(task_queue):
    pubsub = redis_client.my_redis.pubsub()
    pubsub.subscribe(REDIS_CHANNEL)
    print('Subscribed to Redis tasks channel. Waiting for messages...')
    try:
        for message in pubsub.listen():
            if message['type'] == 'message':
                task_id = message['data']
                task = redis_client.get_task(task_id)
                ser_fn = redis_client.get_function(task['function_id'])
                ser_params = task['payload']
                task_queue.put((task_id, ser_fn, ser_params))
    except KeyboardInterrupt:
        raise


def dispatch_pull(port):

    worker_pool = {}

    task_queue = Queue()
    subscribe_process = Process(target=subscribe, args=(task_queue, ), daemon=True)
    subscribe_process.start()

    context = zmq.Context()
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(f'tcp://localhost:{port}')

    while True:
        ser_msg = rep_socket.recv_string()
        req_msg = deserialize(ser_msg)
        worker_id = req_msg['worker']
        if req_msg['event'] == REGISTER_WORKER:
            worker_pool[worker_id] = []
            rep_socket.send_string(serialize(RECEIVED_REP))
            print(f'Worker ID: {worker_id} successfully registered!')
        elif req_msg['event'] == FETCH_TASK:
            try:
                task_tup = task_queue.get(block=True, timeout=0.01)
                print('Send Task:', task_tup)
                task_rep = {
                    'event': SEND_TASK,
                    'data': task_tup
                }
                rep_socket.send_string(serialize(task_rep))
                worker_pool[worker_id].append(task_tup[0])
                redis_client.update_task(task_tup[0], status=TASK_RUNNING)
            except queue.Empty:
                rep_socket.send_string(serialize(RECEIVED_REP))
        elif req_msg['event'] == REPORT_RESULT:
            tup = req_msg['data']
            print('Report Result:', tup)
            worker_pool[worker_id].remove(tup[0])
            report_result(tup)
            rep_socket.send_string(serialize(RECEIVED_REP))
        elif req_msg['event'] == SHUT_DOWN:
            print(f'Worker ID: {worker_id} Shut Down')
            rep_socket.send_string(serialize(RECEIVED_REP))
            for task_id in worker_pool[worker_id]:
                report_result((task_id, TASK_FAILED, serialize('Worker Shut Down')))
            del worker_pool[worker_id]
        else:
            rep_socket.send_string(serialize(RECEIVED_REP))


def dispatch_push(port):
    worker_pool = {}
    inactive_worker = Queue()
    manager = Manager()
    worker_time = manager.dict()

    task_queue = Queue()
    subscribe_process = Process(target=subscribe, args=(task_queue,), daemon=True)
    subscribe_process.start()

    context = zmq.Context()
    router = context.socket(zmq.ROUTER)
    router.bind(f'tcp://localhost:{port}')

    while True:
        try:
            byte_msg = router.recv_multipart(zmq.NOBLOCK)
            worker_id = byte_msg[0].decode()
            msg = deserialize(byte_msg[1].decode())
            worker_time[worker_id] = time.time()
            if msg['event'] == REGISTER_WORKER:
                worker_pool[worker_id] = {
                    'task_list': [],
                    'num_process': msg['data']
                }
                print(f'Worker ID: {worker_id} successfully registered!')
                print(worker_pool)
            elif msg['event'] == REPORT_RESULT:
                tup = msg['data']
                print('Report Result:', tup)
                report_result(tup)
                worker_pool[worker_id]['task_list'].remove(tup[0])
        except zmq.Again:
            pass
        finally:
            while not inactive_worker.empty():
                worker_id = inactive_worker.get()
                print(f'Worker ID: {worker_id} is inactive and will not be assigned tasks!')
                failed_tasks = worker_pool[worker_id]['task_list']
                for task_id in failed_tasks:
                    report_result((task_id, TASK_FAILED, serialize('Worker Failure')))
                del worker_pool[worker_id]
            while (not task_queue.empty()) and len(worker_pool) != 0:
                next_worker_id = min(worker_pool, key=lambda x: len(worker_pool[x]['task_list']) / worker_pool[x]['num_process'])
                task = task_queue.get()
                worker_pool[next_worker_id]['task_list'].append(task[0])
                print(worker_pool)
                router.send_multipart([next_worker_id.encode(), serialize(task).encode()])


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Task Dispatcher")
    parser.add_argument('-m', '--mode', choices=['local', 'pull', 'push'], required=True, help='Mode: local, pull, or push')
    parser.add_argument('-p', '--port', type=int, help='Port number, only for pull or push mode')
    parser.add_argument('-w', '--worker', type=int, help='Number of workers, only for local mode')
    args = parser.parse_args()

    if args.mode == 'local':
        dispatch_local(args.worker)
    elif args.mode == 'pull':
        dispatch_pull(args.port)
    elif args.mode == 'push':
        dispatch_push(args.port)


