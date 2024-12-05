from multiprocessing import current_process

from constants import *
from serialize_deserialize import deserialize, serialize


def execute_task(task_id, ser_fn, ser_params):
    fn = deserialize(ser_fn)
    params = deserialize(ser_params)
    try:
        result = fn(*params[0], **params[1])
        status = TASK_COMPLETE
    except Exception as e:
        err_type = type(e)
        result = f'Function Execute Error: {err_type}. Detail: {e}'
        print(result)
        status = TASK_FAILED
    finally:
        print(current_process().name, 'Result:', task_id, status, result)
        return task_id, status, serialize(result)

