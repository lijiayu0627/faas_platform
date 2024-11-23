import codecs
import json
import redis
import dill


def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


if __name__ == "__main__":

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe('tasks')
    print(f'Subscribed to task channel. Waiting for messages...')
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            # {"task_id": "95767172-a76d-11ef-9fb9-fe4fa44e0234", "function_id": "63cd728c-a70a-11ef-9424-fe4fa44e0234", "status": "QUEUED", "payload": "{\"args\": [1, 2], \"kwargs\": {}}", "result": 0}
            msg = json.loads(message['data'])
            print(f'Received: {msg}')
            r.hset(f'task:{msg["task_id"]}', 'status', 'RUNNING')
            print(r.hgetall(f'task:{msg["task_id"]}'))
            func_str = r.get(f'function:{msg["function_id"]}')
            func = deserialize(func_str)
            payload_dict = json.loads(msg['payload'])
            result = func(*payload_dict['args'], **payload_dict['kwargs'])
            r.hset(f'task:{msg["task_id"]}', 'result', json.dumps(result))
            r.hset(f'task:{msg["task_id"]}', 'status', 'COMPLETE')
            print(r.hgetall(f'task:{msg["task_id"]}'))





