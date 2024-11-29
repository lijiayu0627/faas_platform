import redis_client
from serialize_deserialize import deserialize, serialize


def execute_task(task_id):
    redis_client.update_task(task_id, status='RUNNING')
    task = redis_client.get_task(task_id)
    fn_id = task['function_id']
    fn = deserialize(redis_client.get_function(fn_id))
    params = deserialize(task['payload'])
    try:
        result = fn(*params[0], **params[1])
        status = 'COMPLETE'
    except Exception as e:
        err_type = type(e)
        result = f'Error: {err_type}. Detail: {e}'
        status = 'FAILED'
    finally:
        redis_client.update_task(task_id, status=status, result=serialize(result))

