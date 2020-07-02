import multiprocessing
import threading
from export_pmgrpcd import Exporter
import signal
import lib_pmgrpcd
import logging
import logging.handlers

class WorkerProcess(multiprocessing.Process):
    '''
    Multiprocess handler. Builds the metric processor, and 
    passes metrics to it.
    '''
    def __init__(self, worker_id, queue, state_builder, transform_function, logger_queue):
        super().__init__()
        self.worker_id = worker_id
        self.__queue = queue
        self.__state_builder = state_builder
        self.__transformFunction = transform_function
        self.logger_queue = logger_queue
        self.build_logger()

    def build_logger(self):
        # https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
        h = logging.handlers.QueueHandler(self.logger_queue)  # Just the one handler needed
        self.logger  = logging.getLogger(f"WORKER_{self.worker_id}")
        self.logger.addHandler(h)
        # send all messages, for demo; no other level or filter logic applied.
        self.logger.setLevel(lib_pmgrpcd.get_logger_level())

    def run(self):
        state = self.__state_builder(logger=self.logger)
        while True:
            submission = self.__queue.get(block=True)
            if submission is not None:
                ret = self.__transformFunction(state, submission.job)
                if submission.callback is not None:
                    submission.callback(ret)
            else:
                return

class WorkerTask:
    def __init__(self, job, callback):
        super().__init__()
        self.job = job
        self.callback = callback

class WorkerSwarm:
    '''
    Handler of the multiple processes.
    Starts them, stops them, and handles the task queue.
    '''
    def enqueue(self, job, callback=None):
        self.__queue.put(WorkerTask(job, callback))

    def __init__(self, number_of_workers, state_builder, transform_function, options, logger_queue):
        self.__queue = multiprocessing.Queue()
        self.__logger_queue = logger_queue

        self.__processes = []
        for i in range(0, number_of_workers):
            t = WorkerProcess(str(i), self.__queue, state_builder, transform_function, self.__logger_queue)
            self.__processes.append(t)

    def start(self):
        for process in self.__processes:
            process.start()

    def wait(self):
        for process in self.__processes:
            process.join()

    def stop(self):
        self.__queue.empty()
        for process in self.__processes:
            self.__queue.put(None, block=True)


class LoggingThread(threading.Thread):
    def __init__(self, queue, logger):
        self.queue = queue
        self.logger = logger
        super().__init__()

    def run(self):
        while True:
            record = self.queue.get(block=True)
            if record is not None:
                self.logger.handle(record)
            else:
                return


def processor(state, data):
    return state.process_metric(data)


class MultiProcessingExporter(Exporter):
    def __init__(self, exporter_constructor, transform_function=None, options=None, logger=None):
        super().__init__(logger)
        if transform_function is None:
            transform_function = processor
        if options is None:
            options = lib_pmgrpcd.OPTIONS

        self.log_queue = multiprocessing.Queue()
        self.ws = WorkerSwarm(lib_pmgrpcd.OPTIONS.ProcessPool, exporter_constructor, transform_function, options, self.log_queue)

        # the multiple process log to this queu (which is multiprocess), but 
        # we log in a thread, logging should be thread-safe.
        self.logging_thread = LoggingThread(self.log_queue, self.logger)
        self.logging_thread.start()

        # Define the function used to 
        def term(sig, frame):
            self.stop()

            if self.__prev_handler != 0:
                self.__prev_handler(sig, frame)

            os._exit(0)

        self.ws.start()

        self.__prev_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, term)

    def stop(self):
        self.logger.info("Stopping MultiProcessingExporter")
        self.ws.stop()
        self.ws.wait()

        # stopping logger
        self.log_queue.put(None)
        self.logging_thread.join()


    def process_metric(self, datajsonstring):
        self.ws.enqueue(datajsonstring)
