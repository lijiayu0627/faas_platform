
TASK_QUEUED = 'QUEUED'
TASK_RUNNING = 'RUNNING'
TASK_COMPLETE = 'COMPLETE'
TASK_FAILED = 'FAILED'

REDIS_CHANNEL = 'tasks'

REGISTER_WORKER = 'REGISTER_WORKER'
FETCH_TASK = 'FETCH_TASK'
REPORT_RESULT = 'REPORT_RESULT'
SHUT_DOWN = 'SHUT_DOWN'
RECEIVED = 'RECEIVED'
RECEIVED_REP = {
    'event': RECEIVED,
    'data': ''
}
SEND_TASK = 'SEND_TASK'
Heartbeat = 'Heartbeat'

