import sys
import time
import uuid
import zmq
from multiprocessing import Pool, Lock
from execute_task import execute_task
from constants import *
from serialize_deserialize import serialize, deserialize


def fetch_task(timeout=0.01):
    time.sleep(timeout)
    req = {
        'event': FETCH_TASK,
        'data': '',
        'worker': worker_id
    }
    ser_req = serialize(req)
    with lock:
        req_socket.send_string(ser_req)
        ser_task_rep = req_socket.recv_string()
    rep = deserialize(ser_task_rep)
    if rep['data'] == '':
        return None
    return rep['data']


def report_result(tup):
    req = {
        'event': REPORT_RESULT,
        'data': tup,
        'worker': worker_id
    }
    ser_req = serialize(req)
    with lock:
        req_socket.send_string(ser_req)
        req_socket.recv_string()


def shut_down(err):
    print(err)
    req = {
        'event': SHUT_DOWN,
        'data': '',
        'worker': worker_id
    }
    ser_req = serialize(req)
    req_socket.send_string(ser_req)
    req_socket.recv_string()


if __name__ == '__main__':

    worker_num = int(sys.argv[1])
    dispatcher_url = sys.argv[2]

    context = zmq.Context()
    req_socket = context.socket(zmq.REQ)
    req_socket.connect(dispatcher_url)

    lock = Lock()
    worker_id = str(uuid.uuid1())

    register_req = {
        'event': REGISTER_WORKER,
        'data': '',
        'worker': worker_id
    }
    try:
        req_socket.send_string(serialize(register_req))
        ser_rep = req_socket.recv_string()
        print('Register the worker successfully!')
    except Exception as e:
        print('Register the worker unsuccessfully. Shut down the worker now ...')
        print(e)
        req_socket.close()
        context.term()
        sys.exit(1)

    with Pool(processes=worker_num) as pool:
        try:
            while True:
                tup = fetch_task()
                if tup:
                    pool.apply_async(execute_task, tup, callback=report_result)
        except KeyboardInterrupt:
            shut_down("KeyboardInterrupt received. Shutting down...")
        except zmq.ZMQError as zmq_error:
            shut_down(f"ZeroMQ error encountered: {zmq_error}. Shutting down...")
        except Exception as e:
            shut_down('Shutting down...' + e)
        finally:
            pool.terminate()
            pool.join()
            req_socket.close()
            context.term()