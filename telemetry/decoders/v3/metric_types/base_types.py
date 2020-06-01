from abc import ABC, abstractmethod
from typing import Iterable, Sequence, Dict, Union, Any, Generator, Tuple
import ujson as json
from pygtrie import CharTrie
from utils import generate_content_from_raw
import base64


# Types
ValueField = Dict[str, Union[str, float]]
Field = Dict[str, Union["Field", ValueField]]

class TelemetryException(Exception):
    pass


class MetricException(TelemetryException):
    pass

class KeyErrorMetric(MetricException):
    pass

class SubTreeData(ABC):
    '''
    Represents a generic subtree of data. This describes the most generic
    interface, and should be the supported by all metrics.
    '''

    # Some basic metadata
    @property
    @abstractmethod
    def collection_end_time(self):
        '''
        Timestamp for end of collection.
        If the source does not support it, it could be the same as timestamp.
        '''
        raise NotImplementedError

    @property
    @abstractmethod
    def collection_start_time(self):
        '''
        Timestamp of the start of the collection.
        If the source does not support it, it could be the same as collection_timestamp.
        '''
        raise NotImplementedError

    @property
    @abstractmethod
    def timestamp(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def collection_id(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def path(self):
        '''
        Point of the model from which the data subtree is located.
        '''
        raise NotImplementedError

    @property
    @abstractmethod
    def node(self):
        '''
        Id of node originating the metric. 
        '''
        raise NotImplementedError

    @property
    @abstractmethod
    def subscription_id(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def encoding_type(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def content(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def data(self):
        '''
        The raw data of the metric.
        '''
        raise NotImplementedError

    @property
    @abstractmethod
    def headers(self):
        '''
        Metadata of the metric
        '''
        raise NotImplementedError




# Since we normally  store the data of the metrics in a python dict,
# the next classes provide a shortcut for fetching most of the 
# required properties from that dict.

# Many of the infrastructure of the next classes is about 
# mapping a key to a property. We could do that with one 
# of the libraries (see https://stackoverflow.com/questions/4984647/accessing-dict-keys-like-an-attribute), but since we only take a subset of keys, it is read only, and we need to map the properties to different keys, then I ended up doing my own.

# the base is the next decorator that transform a function returning 
# the key to something that returns the value.
def load_data_and_make_property(f):
    '''
    We Ignore the content, we just add the load
    '''
    def wrapper(self, *args, **kargs):
        key = f(self, *args, **kargs)
        return self.load_from_data(key)
    return property(wrapper)

class DictSubTreeData(SubTreeData):
    '''
    Basic metric in which the data is a dict, where:
    Headers are all values except for content and keys.
    The keys in the dict for the standard attributes can be 
    modified using the properties below.
    '''

    content_key = "content"
    p_key = "encoding_path"
    node_key = "node_id"
    timestamp_key = "timestamp"
    collection_end_time_key = "collection_end_time"
    collection_id_key = "collection_id"
    collection_start_time_key = "collection_start_time"
    encoding_type_key = "encoding_type"
    subscription_id_key = "subscription_id"

    def __init__(self, data: Dict[Any, Any]):
        self._data = data

    @property
    def data(self):
        return self._data



    @property
    def content(self):
        return self.load_from_data(self.content_key, "content")

    @property
    def node(self):
        return self.load_from_data(self.node_key, "node")

    @property
    def timestamp(self):
        return self.load_from_data(self.timestamp_key, "timestamp")

    @property
    def collection_end_time(self):
        return self.load_from_data(self.collection_end_time_key, "collection_end_time")

    @property
    def collection_id(self):
        return self.load_from_data(self.collection_id_key, "collection_id")

    @property
    def collection_start_time(self):
        return self.load_from_data(self.collection_start_time_key, "collection_start_time")

    @property
    def encoding_type(self):
        return self.load_from_data(self.encoding_type_key, "encoding_type")

    @property
    def subscription_id(self):
        return self.load_from_data(self.subscription_id_key, "subscription_id")

    @property
    def headers(self):
        return {x:y for x,y in self.data.items() if x != self.content_key}

    @property
    def path(self) -> str:
        """
        The path (aka as sensor path, encoding path) is the path from where you are getting the data
        """
        return self.load_from_data(self.p_key, "path")

    @staticmethod
    def form_encoding_path(encoding_path: str, levels: Sequence[str]) -> str:
        if not levels:
            return encoding_path
        # if there is no encoding path, then we just return the levesl
        if not encoding_path:
            return "/".join(levels)
        if encoding_path and encoding_path[-1] == "/":
            encoding_path = encoding_path[:-1]
        return "/".join([encoding_path, "/".join(levels)])

    def to_json(self):
        return json.dumps(self.data)

    @classmethod
    def from_json(cls, json_string):
        data = json.loads(json_string)
        return cls(data)


    def load_from_data(self, key, name=None):
        '''
        Loads from the internal data.
        '''
        if name is None:
            name = key
        if key not in self.data:
            raise KeyErrorMetric(f"Error getting {name}, no key {key}")
        return self.data[key]

# The next could also be its own abstract class from SubTreeData, but to avoid multiple inheritance, we wont use it.
class DictElementData(DictSubTreeData):
    '''
    An element is a particular subtree with only one element (e.g. an interface, a qos queue, a fan, etc.)
    Many of the "standard" operations we provide only apply to metrics that contain a single element.
    TSDBs actually store elements (metrics): an entity represented by a key (e.g. labels), ana a value (although you can normally send many values in the same payload)
    '''
    keys_key = "keys"

    @property
    def keys(self):
        return self.load_from_data(self.keys_key, "keys")


class GrpcRaw(DictSubTreeData):
    '''
    This is a general grpc metric, obtained through the grpc definitions from any vendor.
    Content is the grpc msg, metadata comes from the grpc connection and it should be in bytes.
    '''
    
    @classmethod
    def from_base64(cls, data: str, metadata=None) -> "GrpcRaw":
        if metadata is None:
            metadata = {}
        byte_data = base64.b64decode(data)
        metric_data = metadata.copy()
        metric_data["content"] = byte_data
        return cls(metric_data)
    
