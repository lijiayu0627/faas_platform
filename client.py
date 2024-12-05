
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from serialize_deserialize import serialize, deserialize


def two_sum(a, b):
    return a + b


def two_sum_sleep(a, b):
    time.sleep(2)
    return a + b


def execute_task(idx, req):
    response = requests.post("http://127.0.0.1:8000/execute_function", json=req)
    task_rep = response.json()
    return idx, task_rep


def get_result(idx, task_id):
    result_rep = requests.get(f"http://127.0.0.1:8000/result/{task_id}")
    return idx, result_rep.json()

if __name__ == "__main__":

    func_str = serialize(two_sum)
    register_fn_req = {
        'name': 'two_sum',
        'payload': func_str
    }
    fn_rep = requests.post("http://127.0.0.1:8000/register_function", json=register_fn_req)
    fn_id = fn_rep.json()['function_id']

    task_num = 20
    param_list = []
    execute_fn_req_list = []
    expected_result_list = []
    for i in range(task_num):
        a = random.randint(1, 20)
        b = random.randint(1, 20)
        param_list.append((a, b))
        payload = ((a, b), {})
        execute_fn_req = {
            'function_id': str(fn_id),
            'payload': serialize(payload)
        }
        execute_fn_req_list.append(execute_fn_req)
        expected_result_list.append(a+b)

    task_id_list = [0] * task_num
    actual_result_list = [0] * task_num
    with ThreadPoolExecutor(max_workers=5) as executor:
        tasks = [executor.submit(execute_task, idx, execute_fn_req) for idx, execute_fn_req in enumerate(execute_fn_req_list)]

        for task in as_completed(tasks):
            res = task.result()
            if res:
                task_id_list[res[0]] = res[1]['task_id']

        time.sleep(2)
        results = [executor.submit(get_result, idx, task_id) for idx, task_id in enumerate(task_id_list)]

        for result in as_completed(results):
            res = result.result()
            if res:
                actual_result_list[res[0]] = deserialize(res[1]['result'])

        print('Expected Result: ', expected_result_list)
        print('Actual Result: ', actual_result_list)
