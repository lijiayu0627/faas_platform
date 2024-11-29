import uuid

import redis

from serialize_deserialize import deserialize

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)


def save_function(payload):
    try:
        deserialize(payload)
    except:
        raise ValueError
    func_id = uuid.uuid1()
    redis_client.set(f'function:{func_id}', payload)
    return func_id


def get_function(fn_id):
    return redis_client.get(f'function:{fn_id}')


def save_task(fn_id, payload):
    task_id = uuid.uuid1()
    task = {
        'task_id': str(task_id),
        'function_id': str(fn_id),
        'status': 'QUEUED',
        'payload': payload,
        'result': 'NO RESULT',
    }
    redis_client.hset(f'task:{task_id}', mapping=task)
    redis_client.publish('tasks', str(task_id))
    return task_id


def get_task(task_id):
    return redis_client.hgetall(f'task:{task_id}')


def update_task(task_id, **kwargs):
    if 'status' in kwargs:
        redis_client.hset(f'task:{task_id}', 'status', kwargs['status'])
    if 'result' in kwargs:
        redis_client.hset(f'task:{task_id}', 'result', kwargs['result'])

