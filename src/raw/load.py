from os.path import abspath, exists, isdir, isfile, walk, join, basename, expanduser, getsize
import io
from utils.pubsub import PubSub
import struct
from wire.ttypes import TBinaryProtocol
from wire.ttypes import Message
from thrift.transport import TTransport


class DataLocation:
    """A DataLocation represents where data resides on disk"""
    
    ignore_paths = ['rdiff-backup-data']
    
    def __init__(self, directory):
        """
        Arguments:
        directory    -- A directory path string
        """
        self.location = abspath(expanduser(directory))
        assert exists(self.location), "The given path does not exist"
        assert isdir(self.location), "The given path is not a directory"

    @staticmethod
    def sort_paths_by_size(paths):
        """
        Sort the list of paths by the size of the files. Directories are filtered out.
        """
        files = [p for p in paths if isfile(p)]
        files_with_size = [(p, getsize(p)) for p in files]
        files_sorted = sorted(files_with_size, key=lambda x: x[1], reverse=True)
        return [f[0] for f in files_sorted]


    def list_files(self, sort_by_size=False):
        """
        :param sort_by_size: instead of listing the files by creation date, return them sorted by size.
        This is useful for distributing work on the cluster, where we want the biggest files to be
        processed first. N.B. This should only be used with the sessions, and not with the raw data, for
        there we want to maintain temporal order.
        """
        files = []
        def visit(fs, directory, names):
            for ip in DataLocation.ignore_paths:
                if ip in names:
                    names.remove(ip)
            fs.extend([n for n in [join(directory, m) for m in names] if isfile(n) and basename(n)[0] != '.'])
        walk(self.location, visit, files)
        if sort_by_size:
            return self.sort_paths_by_size(files)
        else:
            return files
        
    def count_files(self):
        return len(self.list_files())
 


class LoaderBase(PubSub):
    """"Load data from file and for each object notify subscribers"""

    PUBLISH_BOTH = 2
    PUBLISH_HEADER = 1
    PUBLISH_NONE = 0

    def __init__(self, selector=None):
        PubSub.__init__(self)
        self.selector = selector

    def on_finish(self):
        pass

    def read_single(self, fp):
        with io.open(fp, "rb", 2**16) as buf:
            transport = TTransport.TFileObjectTransport(buf)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)

            try:
                while True:
                    hbs = buf.read(48) # Header is 48 bytes long, see scala code
                    if len(hbs) != 48: break # Stop reading from this file if there aren't enough bytes
                    header = struct.unpack_from('>qqqqhhiii', hbs)
                    if self.selector == None \
                        or self.selector == self.PUBLISH_BOTH \
                        or (hasattr(self.selector, '__call__') and self.selector(header) == self.PUBLISH_BOTH):
                        msg = Message()
                        msg.read(protocol)
                        self.publish((header, msg))
                    else:
                        buf.seek(Header.size(header), io.SEEK_CUR)
                        if self.selector == self.PUBLISH_HEADER \
                            or (hasattr(self.selector, '__call__') and self.selector(header) == self.PUBLISH_HEADER):
                            self.publish((header, None))
            except EOFError, e:
                print 'Encountered unexpected EOF in file %s' % fp

    def read(self):
        raise Exception("Implement!")


class Loader(LoaderBase):
    """"Load data from files provided by a DataLocation"""
    
    def __init__(self, dl, selector=None):
        """
        The loader goes through all files at the given data location and loads a header and 
        then the corresponding message. The selector determines what is published.
        
        Arguments:
        dl        -- a DataLocation
        selector  -- a function that determines what is published
        """
        LoaderBase.__init__(self, selector=selector)
        self.dl = dl
        self.handled_files = 0

    def read(self):
        for fp in self.dl.list_files():
            self.handled_files += 1
            self.read_single(fp)

        # Notify everyone we've finished
        self.finish()


class LoaderSingle(LoaderBase):
    """Load only a single file"""

    def __init__(self, fp, selector=None):
        """
        The loader goes through all files at the given data location and loads a header and
        then the corresponding message. The selector determines what is published.

        Arguments:
        fp        -- a file path (string)
        selector  -- a function that determines what is published
        """
        LoaderBase.__init__(self, selector=selector)
        self.fp = fp

    def read(self):
        self.read_single(self.fp)
        # Notify everyone we've finished
        self.finish()


class Header:
    """
    The Header class is useful for dealing with header tuples.
    """

    def __init__(self):
        raise Exception("Bad boy!")
    
    @staticmethod
    def timestamp_server(t): return t[0]
    
    @staticmethod
    def timestamp_client(t): return t[1]
    
    @staticmethod
    def user(t): return t[2]
    
    @staticmethod
    def size(t): return t[3]
    
    @staticmethod
    def type_metadata(t): return t[4]
    
    @staticmethod 
    def type_data(t): return t[5]

