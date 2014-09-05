from mpi4py import MPI
from array import array
import pickle, time, random, os, json

RANK_MASTER = 0

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()


class Scheduler(object):

    OUTPUT_PICKLED = '__pickled__'
    OUTPUT_JSON = '__json__'

    RES_FAILED = '__failed__'
    RES_SUCCESS = '__success__'
    RES_WAITING = '__waiting__'
    RES_TODO = '__todo__'

    REQ_AVAILABLE = '___available___'
    REQ_PROCESSING = '__processing__'

    FUNC_QUIT = 2**16-1

    def __init__(self, jobs, tag=0, buf_size=2**16):
        """
        :param jobs: a list of tuples (<function number>, <workload>) whereby
        the function number is an int and workload any pickleable python object
        :param buf_size: the size for the buffer to write the pickled python objects to
        :param tag: sends and receives will be done with this tag
        :return: a Scheduler object
        """
        self.jobs = jobs
        self.num_jobs = len(jobs)
        self.scheduled = 0
        self.tag = tag
        self.buf_size = buf_size
        # a result dictionary for each job
        self.results = [{
                            'status': self.RES_TODO,
                            'result': None # this will be the buffer in which we write the result, allocated later
                        } for i in range(self.num_jobs)]
        # a request dictionary for each worker
        self.requests = [{
                             'status': self.REQ_AVAILABLE,
                             'request': None,
                             'rank': rank,
                             'job': -1
                         } for rank in range(1, size)]

    def run(self):
        # The strategy here is to, in a loop, do a round of scheduling and fill up all the works
        # and then wait for a single result. The effect of this strategy is that after the initial
        # filling of the workers, only one job will be scheduled per loop. This is acceptable
        # because we don't expect many results to become available at the same point in time
        # (otherwise the overhead of using the cluster is way too large for the problem at hand).

        # if we've got jobs to schedule, or if we've got results to wait for
        while not self.__finished_p():
            # schedule work if it is necessary, fill all available workers
            if not self.__all_scheduled_p() and not self.__all_workers_busy_p():
                for request in self.__get_free_workers():
                    # some sanity checks
                    assert request['status'] == self.REQ_AVAILABLE
                    assert request['rank'] != RANK_MASTER
                    assert request['request'] == None
                    # send the work
                    jobId = self.scheduled
                    to_send = pickle.dumps(self.jobs[jobId])
                    comm.Isend(to_send, request['rank'], tag=self.tag)
                    request['status'] = self.REQ_PROCESSING
                    # lazily allocated buffer for receiving the result
                    self.results[jobId]['result'] = array('c', '\0') * self.buf_size
                    request['request'] = comm.Irecv(self.results[jobId]['result'], request['rank'], tag=self.tag)
                    request['job'] = jobId
                    self.results[jobId]['status'] = self.RES_WAITING
                    self.scheduled = self.scheduled + 1
            # wait for any receives that have completed
            if self.__any_workers_busy_p():
                status = MPI.Status()
                MPI.Request.Waitany(self.__get_processing_mpi_requests(), status)
                worker_rank = status.Get_source()
                request = self.requests[worker_rank-1]
                jobId = request['job']
                # set result status
                result = self.results[jobId]
                result['status'] = self.RES_FAILED \
                    if status.error or status.cancelled \
                    else self.RES_SUCCESS
                # interpret result from buffer
                result['result'] = pickle.loads(result['result'][:status.Get_count(MPI.CHAR)].tostring())
                # reset request status
                request['status'] = self.REQ_AVAILABLE
                request['request'] = None
                request['job'] = -1

        assert self.__finished_p()


    def __get_processing_mpi_requests(self):
        return [request['request'] for request in self.requests if request['status'] == self.REQ_PROCESSING]

    def __get_processing_workers(self):
        return [request for request in self.requests if request['status'] == self.REQ_PROCESSING]

    def __get_free_workers(self):
        return [request for request in self.requests if request['status'] == self.REQ_AVAILABLE]

    def __all_workers_busy_p(self):
        return all([request['status'] == self.REQ_PROCESSING for request in self.requests])

    def __any_workers_busy_p(self):
        return any([request['status'] == self.REQ_PROCESSING for request in self.requests])

    def __all_scheduled_p(self):
        # some sanity checks
        assert self.scheduled <= self.num_jobs
        assert self.scheduled < self.num_jobs \
               or all([result['status'] != self.RES_TODO for result in self.results])
        return self.scheduled == self.num_jobs

    def __all_results_retrieved_p(self):
        return all([result['status'] == self.RES_FAILED or result['status'] == self.RES_SUCCESS \
                    for result in self.results])

    def __finished_p(self):
        return self.__all_scheduled_p() and self.__all_results_retrieved_p()

    def all_results_successful_p(self):
        return all([result['status'] == self.RES_SUCCESS for result in self.results])

    def get_results(self):
        return self.results

    def get_result_values(self):
        return [result['result'] for result in self.results]


    @staticmethod
    def shutdown_workers(tag=0):
        """
        Shut down all workers with the given tag
        :param tag: only workers with this tag will be shut down
        :return: None
        """
        buf = array('c', '\0') * 32
        for i in range(1, size):
            comm.Isend(pickle.dumps((Scheduler.FUNC_QUIT, "shutdown")), dest=i, tag=tag)
            request = comm.Irecv(buf, source=i)
            status = MPI.Status()
            request.Wait(status)

    @staticmethod
    def write_results(obj, result_name, output=OUTPUT_PICKLED, directory='~/results/'):
        """
        :param obj: a python object
        :param result_name: a string that will be the basename of the file
        :return:
        """
        if output == Scheduler.OUTPUT_PICKLED:
            output_mode = 'wb'
            output_ext  = 'pickled'
        elif output == Scheduler.OUTPUT_JSON:
            output_mode = 'w'
            output_ext  = 'json'

        with open(os.path.abspath(os.path.expanduser(
                    '%s/%s.%s' % (directory, result_name.split(os.path.sep)[-1], output_ext))),
                  output_mode) as f:

            if output == Scheduler.OUTPUT_PICKLED:
                pickle.dump(obj, f)
            elif output == Scheduler.OUTPUT_JSON:
                json.dump(obj, f)


class Receiver(object):

    def __init__(self, functions,
                 tag=0, buf_size=2**16):
        """
        Create a Receiver instance
        :param functions: a list of functions whereby the index in the list
        corresponds to the number sent by the scheduler.
        :param tag: sends and receives will be done with this tag
        :param buf_size: the buffer size for the pickled python objects
        :return: a Receiver object
        """
        self.functions = functions
        self.buf_size = buf_size
        self.tag = tag

    def run(self):
        """
        Run the worker until it gets the quit message from the scheduler
        :return: None
        """
        while True:
            buf = array('c', '\0') * self.buf_size
            r = comm.Irecv(buf, source=RANK_MASTER, tag=self.tag)
            status = MPI.Status()
            r.Wait(status)
            n = status.Get_count(MPI.CHAR)
            input = pickle.loads(buf[:n].tostring())
            # unpack tuple into function id to call and the input object
            f, obj = input
            if f == Scheduler.FUNC_QUIT:
                comm.Isend("shutting down", RANK_MASTER, tag=self.tag)
                return
            output = pickle.dumps(self.functions[f](obj))
            comm.Isend(output, RANK_MASTER, tag=self.tag)


if __name__ == '__main__':

    # If we are the master then we create a Scheduler for each run of jobs that
    # need to be computed and the results collected again. Each job consists of
    # a function id (int) and a python object that is to be the input of that function.

    # If we are a worker then we create a Receiver and we listen for new input
    # until we are given the quit signal. When creating the Receiver we pass in
    # a list of functions. The function id that is sent by the scheduler determines
    # which of these function is executed (the function id is used as an index of the
    # function list).

    # In this simple example we first do a run of 20 jobs with the `echo` function.
    # Then we do a run of 30 jobs with the `random_sleep` function. Finally, we
    # shutdown the workers.

    if rank == RANK_MASTER:
        FUNC_ECHO = 0
        FUNC_SLEEP = 1
        scheduler = Scheduler([(FUNC_ECHO, i) for i in range(20)])
        scheduler.run()
        print '* MASTER run 1 done (all results %ssuccessfull)' % \
              "" if scheduler.all_results_successful_p() else " NOT"
        print '* MASTER results:', scheduler.get_result_values()
        scheduler = Scheduler([(FUNC_SLEEP, i) for i in range(30)])
        scheduler.run()
        print '* MASTER run 2 done (all results %ssuccessfull)' % \
              "" if scheduler.all_results_successful_p() else " NOT"
        print '* MASTER results:', scheduler.get_result_values()
        Scheduler.shutdown_workers()
        print '* MASTER workers have shut down'
    else:
        def echo(x):
            print 'WORKER %s received %s' % (rank, x)
            return x
        def random_sleep(x):
            t = random.random()
            print 'WORKER %s sleeping for %s' % (rank, t)
            time.sleep(t)
            return t
        receiver = Receiver([echo, random_sleep])
        receiver.run()
