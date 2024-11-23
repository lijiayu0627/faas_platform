import json
import uuid

import dill
import codecs

import requests

from req_resp_types import RegisterFnReq, RegisterFnRep, ExecuteFnReq, ExecuteFnRep, TaskStatusRep, TaskResultRep


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()


def two_sum(a, b):
    return a + b

if __name__ == "__main__":
    # func_name = two_sum.__name__
    # func_str = serialize(two_sum)
    # register_fn_req = RegisterFnReq(name=func_name, payload=func_str)
    # response = requests.post("http://127.0.0.1:8000/register_function", json=register_fn_req.dict())
    # print(response.json())
    # {'function_id': '63cd728c-a70a-11ef-9424-fe4fa44e0234'}
    args = [1, 2]
    kwargs = {}
    payload = {'args': args, 'kwargs': kwargs}
    execute_fn_req = {
        'function_id': '63cd728c-a70a-11ef-9424-fe4fa44e0234',
        'payload': json.dumps(payload)
    }
    response = requests.post("http://127.0.0.1:8000/execute_function", json=execute_fn_req)
    print(response.json())






