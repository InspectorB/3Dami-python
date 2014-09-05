from utils.pubsub import PubSub
from utils.types import TypeInfo
from raw.load import Header

class TypeCounter(PubSub):
    
    def __init__(self):
        PubSub.__init__(self)
        self.counts = {'data': {}, 'metadata': {}}
        self.__result = {'data': {}, 'metadata': {}}
        
    def notify(self, publisher, obj):
        header, _ = obj # assumes obj is a tuple
        dt = Header.type_data(header)
        mdt = Header.type_metadata(header)
        self.counts['data'][dt] = self.counts['data'].get(dt, 0) + 1
        self.counts['metadata'][mdt] = self.counts['metadata'].get(mdt, 0) + 1 

    def on_finish(self):
        for k,v in self.counts['data'].items():
            self.__result['data'][TypeInfo.get_data_type_name(k)] = v
        for k,v in self.counts['metadata'].items():
            self.__result['metadata'][TypeInfo.get_metadata_type_name(k)] = v
        
    def get_result(self):
        return self.__result
