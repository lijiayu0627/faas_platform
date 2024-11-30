import uuid

import redis

from serialize_deserialize import deserialize, serialize

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
my_redis = redis.Redis(host='localhost', port=6379, decode_responses=True)


def save_function(payload):
    try:
        deserialize(payload)
    except:
        raise ValueError
    func_id = uuid.uuid1()
    my_redis.set(f'function:{func_id}', payload)
    return func_id


def get_function(fn_id):
    return my_redis.get(f'function:{fn_id}')


def save_task(fn_id, payload):
    task_id = uuid.uuid1()
    task = {
        'task_id': str(task_id),
        'function_id': str(fn_id),
        'status': 'QUEUED',
        'payload': payload,
        'result': serialize('NO RESULT'),
    }
    my_redis.hset(f'task:{task_id}', mapping=task)
    my_redis.publish('tasks', str(task_id))
    return task_id


def get_task(task_id):
    return my_redis.hgetall(f'task:{task_id}')


def update_task(task_id, **kwargs):
    if 'status' in kwargs:
        my_redis.hset(f'task:{task_id}', 'status', kwargs['status'])
    if 'result' in kwargs:
        my_redis.hset(f'task:{task_id}', 'result', kwargs['result'])

