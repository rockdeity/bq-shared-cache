from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, TimeoutError
import logging
from multiprocessing import Queue, Manager
import sys
import time
import timeout_decorator
import unittest

sys.path.append("..")
from src.multiproc import ProcessQueueConsumer

class Test(unittest.TestCase):

    @timeout_decorator.timeout(1)
    def test_process_exception(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            with self.assertRaises(TestException):
                future = executor.submit(raise_exception, "fail")
                future.result(1)

    @timeout_decorator.timeout(1)
    def test_process_timeout_no_signal(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            future = executor.submit(run_until_stopped, exit_event, 0.5)
            #exit_event.set() process will never get exit signal
            result = None
            with self.assertRaises(TimeoutError):
                result = future.result(0.2)
            self.assertFalse(result)


    @timeout_decorator.timeout(1)
    def test_process_not_done(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            future = executor.submit(run_until_stopped, exit_event, 1.0)
            self.assertFalse(future.done())
            time.sleep(0.2)
            self.assertFalse(future.done())
            exit_event.set()
            time.sleep(0.2)
            self.assertTrue(future.done())


    @timeout_decorator.timeout(0.5)
    def test_process_exit_signal(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            future = executor.submit(run_until_stopped, exit_event, 1.0)
            #time.sleep(0.05)
            exit_event.set()
            result = future.result(0.2)
            self.assertTrue(result)

    # test that we get a timeout from trying to resolve the process future
    # if the process timeout has elapsed before the consumer timeout
    @timeout_decorator.timeout(1)
    def test_process_queue_consumer_timeout_future(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            consumer = ProcessQueueConsumer(
                print_work_item,
                work_queue,
                exit_event,
                0.5,
                0.1
            )
            future = executor.submit(consumer.run)
            result = None
            with self.assertRaises(TimeoutError):
                result = future.result(0.1)
            self.assertFalse(result)

    # TODO: reverse conditions
    # test that we get a timeout from trying to resolve the process future
    # if the consumer timeout has elapsed before the process timeout
    @timeout_decorator.timeout(2)
    def test_process_queue_consumer_timeout_consumer(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            consumer = ProcessQueueConsumer(
                print_work_item,
                work_queue,
                exit_event,
                0.5,
                0.1
            )
            future = executor.submit(consumer.run)
            result = None
            with self.assertRaises(TimeoutError):
                result = future.result(0.1)
            self.assertFalse(result)

    @timeout_decorator.timeout(2)
    # test that we will timeout on a join if the worker isn't done
    def test_process_queue_consumer_timeout_join_if_task_not_done(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            # make sure we take time, but don't timeout on our own
            # the join should cause the timeout
            consumer = ProcessQueueConsumer(
                lambda item: time.sleep(0.5),
                work_queue,
                exit_event,
                2,
                1
            )
            work_queue.put("sleep")
            future = executor.submit(consumer.run)
            #print("here")
            time.sleep(0.1)
            self.assertTrue(not future.done())
            # have the worker finish
            work_queue.put(None)

    @timeout_decorator.timeout(1)
    # test that we exit cleanly if the queue is empty and we
    # have recieved an exit event
    def test_process_queue_consumer_empty_exit_event_exits(self):

        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()

            consumer = ProcessQueueConsumer(
                lambda item: item,
                work_queue,
                exit_event,
                1.0,
                0.1
            )

            exit_event.set()
            future = executor.submit(consumer.run)
            future.result()

    @timeout_decorator.timeout(2)
    # test that exit event is set if one consumer raises an exception
    def test_process_queue_consumer_raise_exception_sets_exit_event(self):

        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            consumer = ProcessQueueConsumer(
                raise_exception,
                work_queue,
                exit_event,
                1.0
            )

            with self.assertRaises(TestException):
                work_queue.put("cause exception")
                future = executor.submit(consumer.run)
                print(future.result())
            self.assertTrue(exit_event.is_set())

    @timeout_decorator.timeout(1)
    # test that inserting none will tell the consumers to close down
    def test_process_queue_consumer_insert_none_consumer_finish_no_timeout(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            consumer = ProcessQueueConsumer(
                lambda item: item,
                work_queue,
                exit_event,
                1.0
            )
            future = executor.submit(consumer.run)
            work_queue.put(None)
            future.result(2)

    # test that joining the work queue while consumers are running
    # resuts in a timeout.
    @timeout_decorator.timeout(2)
    def test_process_queue_consumer_join_non_empty_timeout(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            consumer = ProcessQueueConsumer(
                lambda item: item,
                work_queue,
                exit_event,
                0.1
            )
            future = executor.submit(consumer.run)
            work_queue.put("test")
            with self.assertRaises(TimeoutError):
                future.result(1)

    # test that raising an exception will allow joining on the queue
    # because that means no consumers live to eventually
    # consume the queue
    @timeout_decorator.timeout(2)
    def test_process_queue_consumer_assertion_join_no_timeout(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            consumer = ProcessQueueConsumer(
                lambda item: raise_exception(item),
                work_queue,
                exit_event,
                0.5
            )
            future = executor.submit(consumer.run)
            work_queue.put("raise exception")
            with self.assertRaises(TestException):
                print(future.exception())
                if future.exception() is not None:
                    raise TestException

    # test that consumers will timeout with an empty queue
    @timeout_decorator.timeout(2)
    def test_process_queue_consumer_empty_queue_timeout(self):
        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            consumer = ProcessQueueConsumer(
                lambda item: item,
                work_queue,
                exit_event,
                0.5
            )
            future = executor.submit(consumer.run)
            with self.assertRaises(TimeoutError):
                future.result(0.5)

    @timeout_decorator.timeout(1)
    # test that we can add an item to a healthy consumer
    # and then join on it and expect no timeout
    def test_process_queue_consumer_join_result(self):

        with ProcessPoolExecutor(max_workers=1) as executor:
            manager = Manager()
            exit_event = manager.Event()
            work_queue = manager.Queue()
            output_queue = manager.Queue()

            def output_to_queue(item):
                print("output_to_queue:{}".format(item))
                try:
                    print("output_to_queue 2:{}".format(item))
                    output_queue.put(item)
                except Exception:
                    logging.exception()

            consumer = ProcessQueueConsumer(
                output_to_queue,
                work_queue,
                exit_event,
                0.5,
                0.2
            )
            future = executor.submit(consumer.run)
            work_queue.put("test")
            work_queue.join()
            exit_event.set()
            output_item = output_queue.get(True)
            self.assertEqual(output_item, "test")
            output_queue.task_done()

class TestException(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return 'TestException, {0} '.format(self.message)
        else:
            return 'TestException has been raised'

def run_until_stopped(exit_event, timeout):
    start_time = time.perf_counter()
    while True:
        if exit_event.is_set():
            return True
        current_time = time.perf_counter()
        if current_time - start_time >= timeout:
            return False
        time.sleep(0.01)

def raise_exception(work_item):
    raise TestException

def print_work_item(work_item):
    print("work_item".format(work_item))

def passthrough_work_item(output_queue: Queue):

    def l(item):
        print("passthrough_work_item:{}".format(item))
        try:
            output_queue.put(item)
        except Exception:
            logging.exception()
    return l

if __name__ == '__main__':
    unittest.main()