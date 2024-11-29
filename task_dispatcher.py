import argparse
from multiprocessing import Pool
from  execute_task import execute_task
from redis_client import redis_client


def dispatch_local(worker_num):
    with Pool(processes=worker_num) as pool:
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')
        print('Subscribed to task channel. Waiting for messages...')
        try:
            for message in pubsub.listen():
                if message['type'] == 'message':
                    pool.apply_async(execute_task, (message['data'],))
        except KeyboardInterrupt:
            print('Gracefully shutting down...')
            pool.close()
            pool.join()
        finally:
            pubsub.unsubscribe()
            pubsub.close()


def dispatch_pull(port):
    pass


def dispatch_push(port):
    pass


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


