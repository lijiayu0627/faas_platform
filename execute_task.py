from constants import *
from serialize_deserialize import deserialize, serialize


def execute_task(task_id, ser_fn, ser_params):
    fn = deserialize(ser_fn)
    params = deserialize(ser_params)
    print('Input:', task_id, ser_fn, ser_params)
    try:
        result = fn(*params[0], **params[1])
        status = TASK_COMPLETE
    except Exception as e:
        err_type = type(e)
        result = f'Function Execute Error: {err_type}. Detail: {e}'
        print(result)
        status = TASK_FAILED
    finally:
        print('Result:', task_id, status, serialize(result))
        return task_id, status, serialize(result)

