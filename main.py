import uuid
import redis
from fastapi import FastAPI, HTTPException
from req_resp_types import RegisterFnReq, RegisterFnRep, ExecuteFnReq, ExecuteFnRep, TaskStatusRep, TaskResultRep
from serialize_deserialize import serialize, deserialize

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Hello, FaaS Platform!"}


@app.post("/register_function")
def register_function(register_fn_req: RegisterFnReq):
    payload = register_fn_req.payload
    try:
        deserialize(payload)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload format")
    func_id = uuid.uuid1()
    r.set(f'function:{func_id}', payload)
    return RegisterFnRep(function_id=func_id)


@app.post("/execute_function")
def execute_function(execute_fn_req: ExecuteFnReq):
    task_id = uuid.uuid1()
    task = {
        'task_id': str(task_id),
        'function_id': str(execute_fn_req.function_id),
        'status': 'QUEUED',
        'payload': execute_fn_req.payload,
        'result': 'NO RESULT',
    }
    r.hset(f'task:{task_id}', mapping=task)
    r.publish('tasks', str(task_id))
    return ExecuteFnRep(task_id=task_id)


@app.get("/status/{task_id}")
def retrieve_task_status(task_id: str):
    status = r.hget(f'task:{task_id}', 'status')
    return TaskStatusRep(task_id=task_id, status=status)


@app.get("/result/{task_id}")
def fetch_task_result(task_id: str):
    status, result = r.hmget(f'task:{task_id}', ['status', 'result'])
    return TaskResultRep(task_id=task_id, status=status, result=result)



