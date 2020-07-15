from metric_types.base_types import (
    DictSubTreeData,
    DictElementData,
    load_data_and_make_property,
    AttrNotFound,
    AmbiguousContent,
)


class GNMISubscribeRequest(DictSubTreeData):
    '''
    A GNMISubscribeRequest with some extra headers related to the connection
    '''
    pass


class GNMIUpdate(DictElementData):
    '''
    A GNMI element. Encoding is still pending. We do handle the keys, which are 
    equal for all encodings (seems so, at least)
    '''
    pass

# The transformation between GNMIUpdate and GNMIUpdateDict is where all the fun happens.
# There is where one figures the encoding out, and decodes accordingly. 
# For "compact" type of things, proto_bytes should be filled, and we should retrieve the 
# proto via converting a yang module. 
# For json, one decodses the respective fields in the TypedValue.
# I think filling the scalar values in the TypedValue is also defined as PROTO. 

class GNMIUpdateDict(DictElementData):
    '''
    A GNMI element with content in dict.
    '''

