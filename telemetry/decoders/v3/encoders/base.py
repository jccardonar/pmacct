from abc import ABC, abstractmethod
from typing import Iterable, Sequence, Dict, Union, Any, Generator, Tuple
import ujson as json
from pygtrie import CharTrie


# Types
ValueField = Dict[str, Union[str, float]]
Field = Dict[str, Union["Field", ValueField]]



class BaseMetrics:

    @classmethod
    def has_property(cls, property_name):
        property_obj = getattr(cls, property_name, None)
        if property_obj is None:
            raise Exception(f"Cls {cls} does not have property {property_name}")
        return getattr(property_obj, "__isabstractmethod__", False)

    # Some basic metadata
    @property
    @abstractmethod
    def collection_timestamp(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def collection_end_time(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def collection_start_time(self):
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
        raise NotImplementedError

    @property
    @abstractmethod
    def node(self):
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
    def keys(self):
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
        raise NotImplementedError

class GrpcMetric(ABC):

    @property
    @abstractmethod
    def grpc_headers(self):
        raise NotImplementedError


class BaseEncoding(BaseMetrics, ABC):
    '''
    Basic metric in which the data is a dict, where:
    Headers are all values except for content and keys.
    The keys in the dict for the standard attributes can be 
    modified using the properties below.
    '''

    content_key = "content"
    keys_key = "keys"
    p_key = "encodingPath"
    node_key = "node_id"
    timestamp_key = "timestamp"

    def __init__(self, data: Dict[Any, Any]):
        self.data = data
        try:
            self.content
        except:
            raise BaseEncodingException("Content not found")

    @abstractmethod
    def get_internal(self, *args, **kargs) -> "InternalMetric":
        """
        Gets an internal metric
        """
        pass

    def load_from_data(self, key, name=None):
        if name is None:
            name = key
        if key not in self.data:
            raise GetData(f"Error getting {name}, no key {key}")
        return self.data[key]

    @property
    def content(self):
        return self.load_from_data(self.content_key, "content")

    @property
    def keys(self):
        return self.load_from_data(self.keys_key, "keys")

    @property
    def node(self):
        return self.load_from_data(self.node_key, "node")

    @property
    def timestamp(self):
        return self.load_from_data(self.timestamp_key, "timestamp")

    @property
    def path(self) -> str:
        """
        The path (aka as sensor path, encoding path) is the path from where you are getting the data
        """
        return self.load_from_data(self.p_key, "path")

    @staticmethod
    def form_encoding_path(encoding_path, levels):
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


class TelemetryException(Exception):
    pass


class BaseEncodingException(TelemetryException):
    pass


class GetData(BaseEncodingException):
    pass


class InternalIteratorError(BaseEncodingException):
    pass



class InternalMetric(BaseEncoding):

    def __init__(self, data):
        super().__init__(data)

    def replace(self, content=None, keys=None, path=None):
        new_data = self.data.copy()
        if content is not None:
            new_data[self.content_key] = content
        if keys is not None:
            new_data[self.keys_key] = keys
        if path is not None:
            new_data[self.p_key] = path
        return InternalMetric(new_data)

    def get_json(self):
        new_data = self.data.copy()

        new_content = json.dumps(self.content)
        new_data[self.content_key] = new_content

        if self.keys:
            new_key = json.dumps(self.keys)
            new_data[self.keys_key] = new_key

        return JsonTextMetric(new_data)

    def add_keys(self, current_keys, extra_keys):
        new_keys = current_keys.copy()
        for key, value in extra_keys.items():
            self.add_to_flatten(new_keys, key, value)
        return new_keys

    def add_to_flatten(self, flatten_content, key, value):
        if key in flatten_content:
            current_state = flatten_content[key]
            if isinstance(current_state, list):
                current_state.append(value)
            else:
                current_list = [current_state, value]
                flatten_content[key] = current_list
            return
        flatten_content[key] = value


class MetricExceptionBase(Exception):
    def __init__(self, msg, params):
        super().__init__(msg)
        self.params = params

    def str_with_params(self):
        return f"{super().__str__()}, params{str(self.params)}"




class JsonTextMetric(BaseEncoding):
    """
    Data is a json string.
    """

    def get_internal(self, *args, **kargs):
        new_content = json.loads(self.data.content)
        new_data = self.data.copy()
        new_data[self.content_key] = new_content
        return InternalMetric(new_data, *args, **kargs)


