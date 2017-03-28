from rq import Queue
from redis import Redis
import compare_change
import crossref_push
from sseclient import SSEClient as EventSource
import json
import time
import signal
import logging
from config import REDIS_LOCATION, HEARTBEAT_INTERVAL

logging.basicConfig(filename='logs/input.log', level=logging.INFO, format='%(asctime)s %(message)s')
logging.info('cocytus-input launched')

redis_con = Redis(host=REDIS_LOCATION)

queue = Queue('changes', connection = redis_con, default_timeout = 10) #seconds
logging.info('redis connected')

alarm_interval = HEARTBEAT_INTERVAL # 10 minutes, in prime seconds

def alarm_handle(signal_number, current_stack_frame):
	queue.enqueue(crossref_push.heartbeat)
	logging.info('enqueued heartbeat')
	signal.alarm(alarm_interval)

signal.signal(signal.SIGALRM, alarm_handle)
signal.siginterrupt(signal.SIGALRM, False)
signal.alarm(alarm_interval)

for event in EventSource('https://stream.wikimedia.org/v2/stream/recentchange'):
	try:
		if event.event == 'message' and event.data:
			change = json.loads(event.data)
			logging.info(u"enqueing " + str(change))
			queue.enqueue(compare_change.get_changes, change)
		elif event.event == 'error':
			logging.error(event.data)
			time.sleep(1.0)
	except Exception as e:
		logging.error(e.message)
		time.sleep(1.0)
