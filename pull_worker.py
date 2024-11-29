import sys

import zmq





if __name__ == '__main__':
    num_worker_processors = int(sys.argv[1])
    dispatcher_url = sys.argv[2]
    context = zmq.Context()
    pull_socket = context.socket(zmq.REQ)
    pull_socket.connect(dispatcher_url)

