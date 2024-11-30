import json
import time
import uuid

import requests

from serialize_deserialize import serialize, deserialize


def two_sum(a, b):
    return a + b

if __name__ == "__main__":

    func_str = serialize(two_sum)
    register_fn_req = {
        'name': 'two_sum',
        'payload': func_str
    }
    fn_rep = requests.post("http://127.0.0.1:8000/register_function", json=register_fn_req)
    fn_id = fn_rep.json()['function_id']
    print(fn_id)

    payload = ((2, 3), {})
    execute_fn_req = {
        'function_id': str(fn_id),
        'payload': serialize(payload)
    }
    response = requests.post("http://127.0.0.1:8000/execute_function", json=execute_fn_req)
    task_rep = response.json()

    status_rep = requests.get(f"http://127.0.0.1:8000/status/{task_rep['task_id']}")
    print(status_rep.json())

    time.sleep(2)
    result_rep = requests.get(f"http://127.0.0.1:8000/result/{task_rep['task_id']}")
    print(result_rep.json()['status'], deserialize(result_rep.json()['result']))
