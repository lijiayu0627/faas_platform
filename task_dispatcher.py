import redis
from serialize_deserialize import serialize, deserialize


def execute_task(task_id, fn_payload, param_payload):
    fn = deserialize(fn_payload)
    params = deserialize(param_payload)
    try:
        result = fn(*params[0], **params[1])
        task_status = 'COMPLETE'
    except:
        result = 'NO RESULT'
        task_status = 'Failed'
    return task_id, task_status, serialize(result)


if __name__ == "__main__":

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe('tasks')
    print(f'Subscribed to task channel. Waiting for messages...')

    for message in pubsub.listen():
        if message['type'] == 'message':
            task_id = message['data']
            r.hset(f'task:{task_id}', 'status', 'RUNNING')
            task = r.hgetall(f'task:{task_id}')
            print(task)
            func_str = r.get(f'function:{task["function_id"]}')
            task_id, status, result_payload = execute_task(task_id, func_str, task['payload'])
            r.hset(f'task:{task_id}', 'result', result_payload)
            r.hset(f'task:{task_id}', 'status', status)
