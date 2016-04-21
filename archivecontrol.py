import redis
import sys
import json
import time
from pprint import pprint

from redis.exceptions import ConnectionError as RedisConnectionError

redis_url = 'redis://127.0.0.1:16379/0'
pipeline_channel = 'archivebot:pipeline_updates'
log_channel = 'updates'

if len(sys.argv) < 2:
    print('Invalid arguments')
    exit(1)

command = sys.argv[1]

try:
    redis = redis.StrictRedis.from_url(redis_url, decode_responses=True)
except Exception as e:
    print('Failed while connecting to redis at 127.0.0.1:16739: {}'.format(e))
    exit(1)

if sys.argv[1] == 'unregister_pipeline':
    if len(sys.argv) < 3:
        print('Invalid arguments - expect pipeline:xxx after unregister_pipeline')
        exit(1)

    pipeline_id = sys.argv[2]

    redis.delete(pipeline_id)
    redis.srem('pipelines', pipeline_id)
    redis.publish(pipeline_channel, pipeline_id)
    print('Unregistered pipeline with ID {}'.format(pipeline_id))
    exit(0)

elif sys.argv[1] == 'get_settings':
    if len(sys.argv) < 3:
        print('Invalid arguments')
        exit(1)

    job_id = sys.argv[2]

    data = redis.hgetall(job_id)
    print('Data for job ident {}:'.format(job_id))
    print('{}'.format(json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))))
    exit(0)

elif sys.argv[1] == 'get_all_pending_queues':
    for name in redis.scan_iter('pending:*'):
        print('{}'.format(name))

    exit(0)

elif sys.argv[1] == 'dump_working_queue':
    pprint('{}'.format(redis.lrange('working', 0, -1)))
    exit(0)

elif sys.argv[1] == 'force_requeue_job':
    if len(sys.argv) < 3:
        print('Invalid arguments')
        exit(1)

    job_id = sys.argv[2]

    # fetch required job parameter; bail if not found
    data = redis.hgetall(job_id)
    if 'log_key' not in data:
        print('Redis does not have a log key for that job (maybe it does not exist)')
        exit(1)

    # expire queues associated with the job and otherwise mimic the pipeline having marked it done
    redis.hset(job_id, 'pipeline_id', None)
    redis.publish(log_channel, job_id)
    redis.lpush('pending', job_id)

    print('Forced requeue of job {}'.format(job_id))
    exit(0)

# vim:ts=4:sw=4:et:tw=78
