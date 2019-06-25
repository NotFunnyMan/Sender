import requests
import psycopg2 as db


def get_header(conf):
    return {
            'Content-Type': 'application/json',
            'Authorization': 'key=%s' % conf['APP_KEY']
        }


def connect_to_db(conf):
    con_str = f"host={conf['DB_HOST']} port={conf['DB_PORT']} dbname={conf['DB_NAME']} " \
        f"user={conf['DB_USER']} password='{conf['DB_PASS']}'"
    connect = db.connect(con_str)
    connect.autocommit = True
    return connect


def get_users_from_db(users_list, conf):
    db_connect = connect_to_db(conf)
    db_cursor = db_connect.cursor()
    query = '''
    select identificator from devices 
    join rel_users_devices rud on devices.id = rud.device_id
    where rud.user_id '''
    if len(users_list) != 1:
        query += "in %s " % str(tuple(users_list))
    else:
        query += "= %s" % str(users_list).replace('[', '').replace(']', '')
    db_cursor.execute(query)
    devices = db_cursor.fetchall()
    db_connect.close()
    return list(sum(devices, ()))


def perform_task(task, conf):
    users = get_users_from_db(task['users'], conf)
    header = get_header(conf)
    task['registration_ids'] = users
    res = requests.post(conf['GOOGLE_URL'], headers=header, json=task)
    if res.status_code is not requests.codes.ok:
        raise requests.HTTPError('Status code: %s. Reason: %s' % (res.status_code, res.reason))
