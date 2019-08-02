#!/usr/bin/env python3

import json
import time
import random
import requests
import tarantool
from pprint import pprint


def form(s):
    return s + '\n'


def execut_request(function, link, data=None):
    responce = function(url=link, data=json.dumps(data))
    # print(responce.text)
    return json.loads(responce.text)


def tube_create(tube_name):
    return execut_request(
        requests.post,
        f'http://localhost:8081/queue/{tube_name}/create',
        {
            'options': {}
        })


def tube_put(tube_name, data, priority=0):
    return execut_request(
        requests.put,
        f'http://localhost:8081/queue/{tube_name}/put',
        {
            'data': data,
            'priority': priority
        })


def tube_take(tube_name, timeout=None):
    return execut_request(
        requests.get,
        f'http://localhost:8081/queue/{tube_name}/take',
        {
            'timeout': timeout
        })


def tube_delete(tube_name, task_uuid):
    return execut_request(
        requests.delete,
        f'http://localhost:8081/queue/{tube_name}/delete',
        {
            'task_uuid': task_uuid
        })


def tube_release(tube_name, task_uuid):
    return execut_request(
        requests.patch,
        f'http://localhost:8081/queue/{tube_name}/release',
        {
            'task_uuid': task_uuid
        })


def print_table(table):
    for row in table:
        print(row)


def priority_test():
    print('=' * 50)

    tube_name = 'priority_test'
    print(form(tube_name))

    tube_create(tube_name)
    task_count = 100
    tasks_uploaded = []

    print(form('put tasks'))

    start = time.time()
    for i in range(task_count):
        tasks_uploaded.append(
            tube_put(tube_name,
                     data=[i, {'dict': True, 'buffer': 'big string'}],
                     priority=random.randint(0, 100))[0])

    print(form(f'exec time: {time.time() - start}'))

    tasks_taken = []

    print(form('take tasks'))

    start = time.time()

    for i in range(int(task_count / 3)):
        tasks_taken.append(tube_take(tube_name))

    print(form(f'exec time: {time.time() - start}'))
    print(form('ok'))

def delete_test():
    print('=' * 50)

    tube_name = 'delete_test'
    print(form(
        tube_name))
    tube_create(tube_name)

    task_count = 100
    task_deleted_count = 10

    tasks_uploaded = []

    print(form('put tasks'))
    for i in range(task_count):
        tasks_uploaded.append(
            tube_put(tube_name,
                     data=[i, {'dict': True, 'buffer': 'big string'}],
                     priority=random.randint(0, 100))[0])

    random.shuffle(tasks_uploaded)
    task_deleted = tasks_uploaded[:task_deleted_count]
    print(form('delete tasks'))

    for uuid in task_deleted:
        print(uuid)
        tube_delete(tube_name, uuid)

    print(form('take tasks'))
    tasks_taken = []
    for i in range(task_count - task_deleted_count):
        tasks_taken.append(tube_take(tube_name)[0])

    if set(tasks_taken) | set(task_deleted) != set(tasks_uploaded):
        print(form('err'))
    else:
        print(form('ok'))


def release_test():
    print('=' * 50)

    tube_name = 'release_test'
    print(form(tube_name))

    tube_create(tube_name)

    task_count = 100
    task_released_count = 10

    tasks_uploaded = []

    print(form('put tasks'))

    for i in range(task_count):
        tasks_uploaded.append(
            tube_put(tube_name,
                     data=[i, {'dict': True, 'buffer': 'big string'}],
                     priority=random.randint(0, 100))[0])

    random.shuffle(tasks_uploaded)
    tasks_released = tasks_uploaded[:task_released_count]

    print(form('taken all task'))

    for _ in range(task_count):
        tube_take(tube_name)

    print(form(f'release {task_released_count} task'))

    for uuid in tasks_released:
        tube_release(tube_name, uuid)

    print(form('try take released task'))
    tasks_released_take = []

    for _ in range(task_released_count):
        tasks_released_take.append(tube_take(tube_name)[0])

    if set(tasks_released) != set(tasks_released_take):
        print(form('err'))
    else:
        print(form('ok'))


def simple_test():
    print('=' * 50)

    tube_name='simple_test'

    print(form(tube_name))

    tube_create(tube_name)

    task_count=500
    tasks_uploaded=[]

    print(form('put tasks'))

    start=time.time()
    for i in range(task_count):
        tasks_uploaded.append(
            tube_put(tube_name,
                     data=[i, {'dict': True, 'buffer': 'big string'}],
                     priority=random.randint(0, 100))[0])

    print(form(f'exec time: {time.time() - start}'))

    tasks_taken=[]

    print(form('take tasks'))

    start=time.time()

    for i in range(task_count):
        tasks_taken.append(tube_take(tube_name)[0])

    print(form(f'exec time: {time.time() - start}'))

    if set(tasks_uploaded) != set(tasks_taken):
        print(form('err'))
    else:
        print(form('ok'))


if __name__ == '__main__':
    simple_test()

    priority_test()

    delete_test()

    release_test()
