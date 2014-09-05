#!/usr/bin/env python

"""
Please have a look at the simple example first.

In this example we have the same aim as in the simple example, counting the 
number of observations per type of observation. It uses the same 
Publisher/Subscriber design, but employs MPI to distribute the computation 
over several processes and then gather the results.

Don't attempt to run this example simply from the command line; it will number
never finish. Instead, use mpirun, or your cluster's tools, to start the
computation like so:

> mpirun -np $NP exmaple_mpi.py

where $NP is the number of processes to spawn. Use some number greater than
or equal to 2.
"""

from utils.mpi import Scheduler, Receiver, rank, comm, size, RANK_MASTER
from raw.load import LoaderSingle, DataLocation
from features.counters import TypeCounter
from utils import sum_dicts

# One process will take the role of Master. It will distribute the work and
# wait for results.
if rank == RANK_MASTER:
    # Here we use the data split into sessions, rather than the raw files
    dl = DataLocation("~/data/TOC_3Dami_sessions/data/")
    # We won't use the same Loader class as in the simple example. We thus need
    # retrieve all the files we need and use the filename as part of the job specification.
    # Also, we make sure that we sort this list so that the biggest files will be 
    # handled first. That way the files that (we presume to) take the longest won't
    # be scheduled last and prolong the total computation time.
    files = dl.list_files(sort_by_size=True)
    # We create an MPI Scheduler. It is given a list of job tuples of the form 
    # (<func_number>, <job_argument>). The <func_number> specifies which function
    # to run on the the argument, which is obviously specified by <job_argument>.
    # We here thus create a job for each session file and we run the same function
    # on all of them.
    FUNC_NUM = 0
    scheduler = Scheduler([(FUNC_NUM, f) for f in files])
    # Start scheduling! This will block until all results have returned.
    scheduler.run()
    # We shutdown the worker processes.
    Scheduler.shutdown_workers()
    # We get the result values for each job from the scheduler. There should be
    # as many results as jobs scheduled. Note that we reduce all the results
    # with sum_dicts here.
    results = sum_dicts(scheduler.get_result_values())
    # Now we can do with the results what we want
    print results
# As there is only one scheduler, all other spawned processes will be workers
else:
    # We define a function to execute for each job we are given.
    def tc(fp):
        # Like in the simple example we use a loader, but this one will
        # only read a single file, and not all files in a given location.
        # Again, we only publish the headers, and skip reading all the 
        # Thrift messages.
        loader = LoaderSingle(fp, LoaderSingle.PUBLISH_HEADER)
        counter = TypeCounter()
        loader.subscribe(counter)
        loader.read()
        # We simply return the result that we want to send back to the Scheduler.
        return counter.get_result()
    # On the workers we create a receiver and give it a list of functions that 
    # a job specification can refer to. Here we have only one possible function.
    # It is thus the 'tc' function that the FUNC_NUM = 0 refers to.
    receiver = Receiver([tc])
    # Start the receiver! It is now ready to receive jobs from the Master.
    receiver.run()