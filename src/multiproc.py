
from abc import ABC, abstractmethod
import dill
import logging
from multiprocessing import Queue
from queue import Empty
from threading import Event
import time
from typing import Callable, Any

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# will process the work_queue one at a time as input param
# to func until one of the following occurs:
# 1) A None value is passed as the work item
# 2) the exit_event is set
# 3) a timeout has transpired (when timeout is > 0).
class ProcessQueueConsumer:

    def __init__(self,
                 func: Callable[[Any],Any],
                 work_queue: Queue,
                 exit_event: Event,
                 timeout=0.0,
                 poll_interval=1.0,
                 name: str = "unnamed",
                 ):

        self._func_string = dill.dumps(func)
        self._exit_event = exit_event
        self._work_queue = work_queue
        self._timeout = timeout
        self._poll_interval = poll_interval
        self._name = name

    def run(self):
        start_time = time.perf_counter()
        while True:
            work_item = None
            current_time = time.perf_counter()
            if self._timeout > 0 and current_time - start_time >= self._timeout:
                logging.error(f"timeout for consumer:{self._name}")
                raise TimeoutError
            try:
                work_item = self._work_queue.get(block=False)
                try:
                    # this is a signal we should stop working
                    if work_item is None:
                        self._exit_event.set()
                    else:
                        func = dill.loads(self._func_string)
                        func(work_item)
                finally:
                    self._work_queue.task_done()
            except Empty:
                time.sleep(self._poll_interval)
            except Exception as e:
                try:
                    self._exit_event.set()
                finally:
                    logging.exception(f"error while performing work item:{work_item} consumer:{self._name} ...")
                    raise(e)
            finally:
                # wait until after trying to act on work item before checking if we bail in case
                # the exit event was set this frame
                if self._exit_event.is_set():
                    logging.info(f"exit event set for consumer:{self._name}, leaving")
                    return





