from redis import Redis
from rq import Queue
from tasks import perform_task
import logger
import json
from rq.worker import Worker, Job

SENDER_CONF_PATH = 'sender.conf'
REDIS_CONF_PATH = 'redis.conf'


class MYQueue:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MYQueue, cls).__new__(cls)
            MYQueue._instance.conf = MYQueue._get_conf(SENDER_CONF_PATH)
            MYQueue._instance.redis_connect = MYQueue._connect_to_redis()
            MYQueue._instance.default_queue = MYQueue._get_queue(MYQueue._instance.redis_connect, 'default')
        return MYQueue._instance

    @staticmethod
    def _get_conf(file_path):
        logger.info('Reading a json configuration file...')
        try:
            with open(file_path) as f:
                return json.load(f)
        except Exception as e:
            logger.exception('Cannot read json config!\n%s' % e)
            exit(1)

    @staticmethod
    def _connect_to_redis():
        conf = MYQueue._get_conf(REDIS_CONF_PATH)
        logger.info("Connecting to Redis...")
        logger.debug(f"Redis connection string: {conf['REDIS_HOST']}:{conf['REDIS_PORT']}")
        return Redis(host=conf['REDIS_HOST'], port=conf['REDIS_PORT'])

    @staticmethod
    def _get_queue(redis_connect, queue_name):
        logger.info("Getting Redis queue...")
        return Queue(queue_name, connection=redis_connect)

    def add_task_to_queue(self, task):
        job = self.default_queue.enqueue_call(perform_task, args=(task, self.conf), timeout=self.conf['TASK_TIMEOUT'])
        return self.save_task_to_redis(job, task)

    def save_task_to_redis(self, job, data):
        logger.debug(f"Task has been sending to queue: {job}")
        self.redis_connect.set(job.id, '%s' % data)

    def get_task_from_redis(self, job_id):
        # tmp = self.redis_connect.get(name=job_id)
        tmp = Job.fetch(job_id, connection=self.redis_connect)
        return tmp.to_dict()

    def restart_task(self, job_id):
        task = self.get_task_from_redis(job_id)
        task_json = json.loads(task.decode('utf-8').replace("'", '"'))
        return self.add_task_to_queue(task_json)

    def get_worker_statistics(self, worker_name):
        workers = Worker.all(queue=self.default_queue)
        for worker in workers:
            if worker.name == worker_name:
                return dict(total=worker.total_working_time,
                            successful=worker.successful_job_count,
                            failed=worker.failed_job_count
                            )
        return dict(error='Worker not found')

    def get_all_workers_statistics(self):
        workers = Worker.all(queue=self.default_queue)
        res = []
        for worker in workers:
            data = dict(
                total=worker.total_working_time,
                successful=worker.successful_job_count,
                failed=worker.failed_job_count
            )
            d = dict(name=worker.name, stat=data)
            res.append(d)
        return res

    def test_method(self, job_1):
        # from rq.job import Job
        # self.failed_queue.empty()
        # test = self.failed_queue.failed_job_registry
        # job = Job.fetch('67d2bd9f-51ee-43b3-8391-4fbdbbe08e6f', connection=self.redis_connect)
        # self.failed_queue.enqueue_job(job)
        pass
