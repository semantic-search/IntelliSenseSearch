import globals
from redis import Redis
from rq import Queue

redis = Redis()
q = Queue(connection=redis)
print(globals.REDIS_HOSTNAME)
