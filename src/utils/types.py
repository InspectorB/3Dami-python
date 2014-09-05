from wire.data.ttypes import Data
from wire.metadata.ttypes import Metadata

class TypeInfo:
    
    #### Type number to type string
    
    @staticmethod
    def get_type_tuple(tcls, num=None, name=None):
        # either name or num, or both have to be provided
        if num == None and name == None:
            return
        for spec in tcls.thrift_spec:
            if spec == None:
                continue
            elif (spec[0] == num or num == None) and (spec[2] == name or name == None):
                return spec
        return None

    @staticmethod
    def get_type_name(tcls, num):
        t = TypeInfo.get_type_tuple(tcls, num)
        return t[2] if t else None
    
    @staticmethod
    def get_data_type_name(num):
        return TypeInfo.get_type_name(Data, num)
    
    @staticmethod
    def get_metadata_type_name(num):
        return TypeInfo.get_type_name(Metadata, num)

    #### Type string to type number
    
    @staticmethod
    def get_type_number(tcls, name):
        t = TypeInfo.get_type_tuple(tcls, name=name)
        return t[0] if t else None
    
    @staticmethod
    def get_data_type_number(name):
        return TypeInfo.get_type_number(Data, name)
    
    @staticmethod
    def get_metadata_type_number(name):
        return TypeInfo.get_type_number(Metadata, name)
    