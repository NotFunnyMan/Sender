from flask import Flask, request, jsonify
from my_queue import MYQueue

app = Flask(__name__)


@app.route("/")
def hello():
    return '<h1 align=center> Welcome to the Sender API </h1>\n<h3 align=center>Welcome and fuck off :)</h3>'


@app.route("/test")
def test():
    myqueue = MYQueue()
    myqueue.test_method("")
    return "<h1 align=center>Fuck you!!!</h1>"


@app.route("/sender/api/v1/add_task", methods=['POST'])
def add_task():
    myqueue = MYQueue()
    myqueue.add_task_to_queue(request.json)
    return jsonify({'response': 'accepted'})


@app.route("/sender/api/v1/get_task", methods=['GET'])
def get_task():
    myqueue = MYQueue()
    job_id = request.args.get('id')
    job = myqueue.get_task_from_redis(job_id)
    if job is not None:
        return jsonify(job.decode('utf-8').replace("'", '"')).json
    else:
        return jsonify(None)


@app.route("/sender/api/v1/restart_task", methods=['GET'])
def restart_task():
    myqueue = MYQueue()
    job_id = request.args.get('id')
    job = myqueue.restart_task(job_id)
    return "<h1>ID: %s</h1>\n<h2>RESULTS: %s</h2>" % (job.id, job.result)


@app.route("/sender/api/v1/worker_statistics", methods=['GET'])
def worker_statistic():
    myqueue = MYQueue()
    worker_name = request.args.get('worker_name', 'default')
    return jsonify(myqueue.get_worker_statistics(worker_name))


@app.route("/sender/api/v1/all_workers_statistics", methods=['GET'])
def all_workers_statistic():
    myqueue = MYQueue()
    return jsonify(myqueue.get_all_workers_statistics())


if __name__ == '__main__':
    app.run(debug=True)
