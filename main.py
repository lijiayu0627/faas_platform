
from fastapi import FastAPI, HTTPException
from req_resp_types import *
import redis_client

app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Hello, FaaS Platform!"}


@app.post("/register_function")
def register_function(register_fn_req: RegisterFnReq):
    payload = register_fn_req.payload
    try:
        func_id = redis_client.save_function(payload)
        return RegisterFnRep(function_id=func_id)
    except ValueError:
        raise HTTPException(status_code=400, detail='Invalid function payload format')


@app.post("/execute_function")
def execute_function(execute_fn_req: ExecuteFnReq):
    if not redis_client.get_function(execute_fn_req.function_id):
        raise HTTPException(status_code=404, detail=f'Function with ID: {execute_fn_req.function_id} not found')
    task_id = redis_client.save_task(execute_fn_req.function_id, execute_fn_req.payload)
    return ExecuteFnRep(task_id=task_id)


@app.get("/status/{task_id}")
def retrieve_task_status(task_id: str):
    task = redis_client.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f'Task with ID: {task_id} not found')
    return TaskStatusRep(task_id=task_id, status=task['status'])


@app.get("/result/{task_id}")
def fetch_task_result(task_id: str):
    task = redis_client.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f'Task with ID: {task_id} not found')
    return TaskResultRep(task_id=task_id, status=task['status'], result=task['result'])

