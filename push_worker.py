import sys

import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)

if __name__ == '__main__':
    num_worker_processors = int(sys.argv[1])
    dispatcher_url = sys.argv[2]
