from concurrent.futures import CancelledError
from concurrent.futures import ThreadPoolExecutor
import logging
import functools
from datetime import datetime


def p(line):
    print(line)
    pass


def measure_time(func):
    @functools.wraps(func)
    def dec_inner(*args, **kw):
        logging.warning("START")

        start = datetime.now()

        ret = func(*args, **kw)

        end_time = datetime.now()

        total_execution_time = (end_time - start).total_seconds()

        logging.warning(f'DONE in {total_execution_time} seconds')

        return ret
    return dec_inner


@measure_time
def multithread_read_lines():
    with ThreadPoolExecutor(max_workers=30) as executor:
        with open(file='DATA/test.xml') as f:

            for line in f:
                executor.submit(p, line)


@measure_time
def basic_read_lines():
    with open(file='DATA/test.xml') as f:

        for line in f:
            p(line)


def cube(x):
    print(f'Cube of {x}:{x * x * x}')

@measure_time
def multithread_operation():
    result = []

    values = [3, 4, 5, 6]

    with ThreadPoolExecutor(max_workers=5) as exe:
        exe.submit(cube, 2)

        # Maps the method 'cube' with a list of values.
        # result = exe.map(cube, values)

    # for r in result:
    #     print(r)


@measure_time
def base_operation():
    result = []

    values = [3, 4, 5, 6]

    cube(2)

    for r in result:
        print(r)


if __name__ == '__main__':
    multithread_operation()
    base_operation()
#     multithread_read_lines()
#     # basic_read_lines()
