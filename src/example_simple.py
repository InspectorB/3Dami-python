#!/usr/bin/env python

"""
In this example we count the number of operators that have been observed during
the 3Dami summer studio.

The design of the library is based on a Publisher/Subscriber pattern. That way 
we can read the data once, and run it through several analysis chains. It was
designed to allow easy implementation of new feature extractors and reuse of 
several common features. It has no facilities for complex analysis flows.

In this example we only make use of one thread, and we only need to  read the
headers, and not the actual Thrift objects. See the MPI example for how to do
analysis in parallel.
"""

# Import the necessary classes for reading the data
from raw.load import DataLocation, Loader, Header
# Import the TypeCounter from the features packages
from features.counters import TypeCounter
# Import the TypeInfo utility class
from utils.types import TypeInfo

# Set data path. This is the path where the data resides. In this case
# we make use of the raw data, and not the data split into sessions.
dl_path = "/Users/vakkermans/data/TOC_3Dami/data/"

# Get the code for the operator type. The argument corresponds 
# to the field names of the Thrift 'Data' struct. Other valid
# arguments are, for example, 'assignment', 'sessionStart', 
# 'sessionEnd', etc.
wmop_code = TypeInfo.get_data_type_number('wmOp')

# Create the DataLocation that we will pass to the Loader instance
dl = DataLocation(dl_path)
print 'There are %s files at the specified data path' % dl.count_files()

# Create the Loader instance and pass it the DataLocation instance and
# a specification for what to load from the data files. There are
# several possible options:
# 1. Passing Loader.PUBLISH_BOTH will cause the Loader to load both the 
#    header and the Thrift object. It will publish them as a tuple 
#    (<header>, <message>) to any of its subscribers.
# 2. Passing Loader.PUBLISH_HEADER will cause the Loader to load only
#    the header, and it will publish a tuple (<header>, None).
# 3. Passing Loader.PUBLISH_NONE will cause the Loader to only load the
#    header, but it will publish nothing. It still needs to load the header
#    to know how much to skip forward in the file (past the Thrift object).
# 4. Passing a function will cause the Loader to execute that function 
#    to decide what to publish. The function has to return either
#    Loader.PUBLISH_BOTH, Loader.PUBLISH_HEADER, or Loader.PUBLISH_NONE. 
#    This function takes as an argument only a header Tuple. It can, for 
#    example, be used to only publish objects of a certain type.

# We won't use this, but this is just an example of how to only publish
# the headers for 'operator' observations.
def selector(header):
    if Header.type_data(header) == wmev_code:
        return Loader.PUBLISH_HEADER
    else:
        return Loader.PUBLISH_NONE
# loader = Loader(dl, selector)

# Instead, we use the simple form
loader = Loader(dl, Loader.PUBLISH_HEADER)

# So now we've set up the loader to only publish the headers for 
# and it does so for any kind of observation. It publishes these as 
# a (<header>, None) tuple to its subscribers. That means that in 
# order to do anything useful we'll need to create an Instance of 
# some sort of subscriber. Here we'll be using a simple one called 
# TypeCounter().

# We create the typecounter and subscribe it to the loader.
tc = TypeCounter()
loader.subscribe(tc)

# To start reading the data and to start counter observation types
# we tell the loader to start reading.
loader.read()

# We can get the results from the TypeCounter by calling get_result() on it.
tc_data = tc.get_result()['data']

# Let's print the results
for key,val in tc_data.items():
    print '%s %s' % (key.ljust(15), val)