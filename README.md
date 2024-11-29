###Install Dependencies
```buildoutcfg
pip install -r requirements.txt
```
###Start Faas service
```buildoutcfg
uvicorn main:app --reload
```
###Start Redis Server
```buildoutcfg
redis-server
```
###Start Task Dispatcher
```buildoutcfg
python task_dispatcher.py -m [local/pull/push] -p <port> -w <num_worker_processors>
```
###Test
```buildoutcfg
pytest tests
```