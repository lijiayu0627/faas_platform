import sys
import time
import uuid
import zmq
from multiprocessing import Pool
from execute_task import execute_task
from constants import *
from serialize_deserialize import serialize, deserialize

process_num = int(sys.argv[1])
dispatcher_url = sys.argv[2]
worker_id = str(uuid.uuid1())

context = zmq.Context()
dealer = context.socket(zmq.DEALER)
dealer.setsockopt(zmq.IDENTITY, worker_id.encode())
dealer.connect(dispatcher_url)


def report_result(tup):
    print('Report Result:', tup)
    req = {
        'event': REPORT_RESULT,
        'data': tup,
    }
    ser_req = serialize(req)
    dealer.send_string(ser_req)


if __name__ == '__main__':

    register_req = {
        'event': REGISTER_WORKER,
        'data': process_num
    }
    try:
        dealer.send_string(serialize(register_req))
        print('Register the worker successfully!')
    except Exception as e:
        print('Register the worker unsuccessfully. Shut down the worker now ...')
        print(e)
        dealer.close()
        context.term()
        sys.exit(1)

    with Pool(processes=process_num) as pool:
        try:
            while True:
                byte_msg = dealer.recv_multipart()
                tup = deserialize(byte_msg[0].decode())
                if tup:
                    pool.apply_async(execute_task, tup, callback=report_result)

        finally:
            pool.terminate()
            pool.join()
            dealer.close()
            context.term()