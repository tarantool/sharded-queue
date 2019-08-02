#!/usr/bin/env python3

import json
import time
import random
import requests
import tarantool
from pprint import pprint


def execut_request(function, link, data=None):
    responce = function(url=link, data=json.dumps(data))
    print(responce.text)
    return json.loads(responce.text)


def tube_create(tube_name):
    return execut_request(
        requests.post,
        'http://localhost:8081/queue/create',
        {
            'tube_name': tube_name,
            'options': {}
        })


def tube_put(tube_name, data, priority=0):
    return execut_request(
        requests.post,
        'http://localhost:8081/queue/put',
        {
            'data': data,
            'tube_name': tube_name,
            'priority': priority
        })


def tube_take(tube_name, timeout=None):
    return execut_request(
        requests.get,
        f'http://localhost:8081/queue/take/{tube_name}',
        {
            'timeout': timeout
        })


def print_table(table):
    for row in table:
        print(row)


def priority_test():
    tube_name = 'priority_test'
    tube_create(tube_name)
    task_count = 100
    tasks_uploaded = []

    print('==put tasks')
    start = time.time()
    for i in range(task_count):
        tasks_uploaded.append(
            tube_put(tube_name,
                     data=[i, {'dict': True, 'buffer': 'big string'}],
                     priority=random.randint(0, 100))[0])

    print(f'exec time: {time.time() - start}')
    tasks_taken = []

    print('==take tasks')
    start = time.time()

    for i in range(int(task_count / 3)):
        tasks_taken.append(tube_take(tube_name))

    print(f'exec time: {time.time() - start}')


def simple_test():

    print('==create tubes')
    tube_name = 'simple_test'
    tube_create(tube_name)

    task_count = 500
    tasks_uploaded = []

    print('==put tasks')
    start = time.time()
    for i in range(task_count):
        tasks_uploaded.append(
            tube_put(tube_name,
                     data=[i, {'dict': True, 'buffer': 'big string'}],
                     priority=random.randint(0, 100))[0])
    print(f'exec time: {time.time() - start}')

    tasks_taken = []

    print('==take tasks')
    start = time.time()

    for i in range(task_count):
        tasks_taken.append(tube_take(tube_name)[0])

    print(f'exec time: {time.time() - start}')

    if set(tasks_uploaded) != set(tasks_taken):
        print('err')

        print('==tasks_uploaded')
        print_table(tasks_uploaded)
        print('==tasks_taken')
        print_table(tasks_taken)


if __name__ == '__main__':
    simple_test()
    priority_test()
