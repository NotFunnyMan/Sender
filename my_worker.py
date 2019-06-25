import sys
from rq import Worker
from rq.job import Job
from redis import Redis
import traceback
import logger
import json

REDIS_CONF_PATH = 'redis.conf'


def get_conf():
    logger.info('Reading a json configuration file...')
    try:
        with open(REDIS_CONF_PATH) as f:
            return json.load(f)
    except Exception as e:
        logger.exception('Cannot read json config!\n%s' % e)
        exit(1)


def my_handler(job, exc_type, exc_value, exc_traceback):
    logger.error("""
    JOB: {job}
    EXC_TYPE: {type}
    EXC_VALUE: {value}
    TRACEBACK: {traceback}
    """.format(job=job, type=exc_type, value=exc_value, traceback=''.join(traceback.format_tb(exc_traceback))))

    res = redis_connect.get(job.id)
    data = job.to_dict()
    res = res.decode('utf-8').replace("'", '"')
    tmp = json.loads(res)
    data['data'] = tmp['data']
    data.pop('description')
    data['error'] = dict(
                    id=job.id,
                    exc_type=str(exc_type).replace('"', "'").replace("'", '').replace('"', ''),
                    exc_value=str(exc_value).replace('"', "'").replace("'", '').replace('"', ''),
                    traceback=str(traceback.format_tb(exc_traceback)).replace("'", '').replace('"', '')
                        )
    logger.error(data)
    redis_connect.set(job.id, str(data))


conf = get_conf()
redis_connect = Redis(host=conf['REDIS_HOST'], port=conf['REDIS_PORT'])
qs = sys.argv[1:] or ['default']
w = Worker(qs, name='{name}-worker'.format(name=qs[0]), exception_handlers=my_handler, connection=redis_connect)
w.work()
