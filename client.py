import json
import uuid

import requests

from req_resp_types import RegisterFnReq, RegisterFnRep, ExecuteFnReq, ExecuteFnRep, TaskStatusRep, TaskResultRep
from serialize_deserialize import serialize, deserialize


def two_sum(a, b):
    return a + b

if __name__ == "__main__":
    # func_name = two_sum.__name__
    # func_str = serialize(two_sum)
    # register_fn_req = RegisterFnReq(name=func_name, payload=func_str)
    # response = requests.post("http://127.0.0.1:8000/register_function", json=register_fn_req.dict())
    # print(response.json())
    # {'function_id': '63cd728c-a70a-11ef-9424-fe4fa44e0234'}

    '''
    args = [1, 2]
    kwargs = {}
    payload = ((2, 3), {})
    execute_fn_req = {
        'function_id': '63cd728c-a70a-11ef-9424-fe4fa44e0234',
        'payload': serialize(payload)
    }
    response = requests.post("http://127.0.0.1:8000/execute_function", json=execute_fn_req)
    print(response.json())
    '''

    response = requests.get("http://127.0.0.1:8000/status/c8bcc8be-a9fd-11ef-9c6e-fe4fa44e0234")
    print(response.json())
    response = requests.get("http://127.0.0.1:8000/result/c8bcc8be-a9fd-11ef-9c6e-fe4fa44e0234")
    print(deserialize(response.json()['result']))
