###Install Dependencies
```buildoutcfg
pip install -r requirements.txt
```
###Start Faas Web Service
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
eg:
```buildoutcfg
python task_dispatcher.py -m local -w 4
python task_dispatcher.py -m pull -p 5555
python task_dispatcher.py -m push -p 5555
```
###Start Pull Worker When Task Dispatcher is in pull mode
```buildoutcfg
python pull_worker.py <num_worker_processors> <dispatcher url>
```
eg:
```buildoutcfg
python pull_worker.py 4 tcp://localhost:5555
```
###Start Push Worker When Task Dispatcher is in pull mode
```buildoutcfg
python push_worker.py <num_worker_processors> <dispatcher url>
```
eg:
```buildoutcfg
python push_worker.py 4 tcp://localhost:5555
```
###Start Client to Test
```buildoutcfg
python client.py
```
###Test
```buildoutcfg
pytest tests
```