import base64
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, Dict, Optional, Sequence, Union, TypeVar

import ujson as json
from exceptions import PmgrpcdException, MetricException
from .dict_proxy import DictProxy, dict_attribute, AttrNotFound, KeyErrorMetric

# Types
ValueField = Dict[str, Union[str, float]]
Field = Dict[str, Union["Field", ValueField]]

ContainersTypes = (dict, list)


class AmbiguousContent(AttrNotFound):
    pass


class SubTreeData(ABC):
    """
    Represents a generic subtree of data. This describes the most generic
    interface, and should be supported by all metrics.
    """

    @property
    @abstractmethod
    def data(self) -> Any:
        """
        The raw data of the metric. Includes metadata.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def path(self) -> str:
        """
        Container on the model from which the content is originared.
        """
        raise NotImplementedError


    @property
    @abstractmethod
    def content(self) -> Any:
        """
        Actual content of the metric. Data minus the metadata.
        Depending on the type, this could be bytes, str, or a compound type (e.g. dict).
        More specific subtypes could override the type of this property.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def headers(self) -> Dict[str, Any]:
        """
        Metadata of the metric.
        """
        raise NotImplementedError

    # Some basic metadata. This might go somewhere else.
    @property
    @abstractmethod
    def collection_end_time(self):
        """
        Timestamp for end of collection.
        If the source does not support it, it could be the same as timestamp.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def collection_start_time(self):
        """
        Timestamp of the start of the collection.
        If the source does not support it, it could be the same as collection_timestamp.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def msg_timestamp(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def collection_id(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def collection_data(self) -> Dict[str, Any]:
        raise NotImplementedError

    @property
    @abstractmethod
    def node_id(self) -> str:
        """
        Id of node originating the metric. 
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def subscription_id(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def encoding_type(self):
        raise NotImplementedError

    def to_dict(self, normalized=False):
        """
        Returns the data in python dict form.
        """
        raise NotImplementedError

    # A couple of methods that depend on xpath handeling.
    @property
    def module(self) -> Optional[str]:
        """
        Get the module from the fist step on the xpath.
        TODO: We might need a proper xpath parser.
        """
        if ":" in self.path:
            return self.path.split(":")[0]
        return None

    @staticmethod
    def form_encoding_path(encoding_path: str, levels: Sequence[str]) -> str:
        '''
        Forms an xpath by appending levels to a path
        '''
        if not levels:
            return encoding_path
        # if there is no encoding path, then we just return the levesl
        if not encoding_path:
            return "/".join(levels)
        if encoding_path and encoding_path[-1] == "/":
            encoding_path = encoding_path[:-1]
        return "/".join([encoding_path, "/".join(levels)])


# Since we normally  store the data of the metrics in a python dict,
# the next classes provide a shortcut for fetching most of the
# required properties from that dict.


# TypeVar bounded to DictSubTreeData
T = TypeVar("T", bound="DictSubTreeData")

class DictSubTreeData(SubTreeData, DictProxy):
    """
    Basic metric in which the data is a dict. Some assumptions:
        - Headers are all values except for content and keys.

    We support a level of indirection to allow for easily change 
    the key used on the dict for standard metric types.

    That is, to allow for a metric (for instance Huawei), to 
    use another key for the same attribute.

        - The keys in the dict for the standard attributes are
          hard written in the class.
        - We link a metric attribute to the name of its key in  the _attr_to_key
    """

    # Dict linking metric property with class property containing  the name of the key
    _attr_to_key: Dict[str, str] = {
        "content": "content_key",
        "path": "p_key",
        "node_id": "node_key",
        "msg_timestamp": "msg_timestamp_key",
        "collection_start_time": "collection_start_time_key",
        "collection_end_time": "collection_end_time_key",
        "collection_timestamp": "collection_timestamp_key",
        "collection_id": "collection_id_key",
        "encoding_type": "encoding_type_key",
        "subscription_id": "subscription_id_key",
        "collection_data": "collection_data_key",
    }


    def replace(
        self: T,
        content=None,
        keys=None,
        path: Optional[str] = None,
        collection_data=None,
    ) -> T:
        """
        Returns an object of the same type, but replacing some of its data.
        """
        new_data = self.data.copy()
        if content is not None:
            new_data[self.content_key] = content
        if collection_data is not None:
            new_data[self.collection_data_key] = collection_data
        if keys is not None:
            new_data[self.keys_key] = keys
        if path is not None:
            new_data[self.p_key] = path
        return self.__class__(new_data)

    # Some basic terms for the keys are included here.
    content_key = "content"
    p_key = "encoding_path"
    node_key = "node_id_str"
    timestamp_key = "msg_timestamp"
    collection_end_time_key = "collection_end_time"
    collection_id_key = "collection_id"
    collection_start_time_key = "collection_start_time"
    collection_timestamp_key = "collection_timestamp"
    encoding_type_key = "encoding_type"
    msg_timestamp_key = "msg_timestamp"
    subscription_id_key = "subscription_id"
    collection_data_key = "collection_data"

    def __init__(self, data: Dict[Any, Any]):
        self._data = data

    @classmethod
    def from_json(cls, json_string):
        data = json.loads(json_string)
        return cls(data)

    @property
    def data(self) -> Dict[str, Any]:
        return self._data

    @property
    def headers(self) -> Dict[str, Any]:
        return {x: y for x, y in self.data.items() if x != self.content_key}

    @dict_attribute
    def content(self) -> Any:
        pass

    @dict_attribute
    def node_id(self):
        pass

    @dict_attribute
    def collection_timestamp(self):
        pass

    @dict_attribute
    def collection_data(self) -> Dict[str, Any]:
        """
        Collection data is another dict with metadata about the collection.
        This could include time of collection, collector id, etc.
        """
        pass

    @dict_attribute
    def msg_timestamp(self):
        pass

    @dict_attribute
    def collection_end_time(self):
        pass

    @dict_attribute
    def collection_id(self):
        pass

    # @property
    # def collection_start_time(self):
    #    return self.load_from_data(
    #        self.collection_start_time_key, "collection_start_time"
    #    )
    @dict_attribute
    def collection_start_time(self):
        pass

    @dict_attribute
    def encoding_type(self):
        pass

    @dict_attribute
    def subscription_id(self) -> str:
        pass

    @dict_attribute
    def path(self) -> str:
        """
        The path (aka as sensor path, encoding path) is the path from where you are getting the data
        """
        pass

    def to_dict(self):
        return self.data

    def to_json(self):
        return json.dumps(self.data)



# The next could also be its own abstract class from SubTreeData, but to avoid multiple inheritance, we wont use it.
class DictElementData(DictSubTreeData):
    """
    An element is a particular subtree with only one element (e.g. an interface, a qos queue, a fan, etc.)
    Many of the "standard" operations we provide only apply to metrics that contain a single element.
    TSDBs actually store elements (metrics): an entity represented by a key (e.g. labels), ana a value (although you can normally send many values in the same payload)
    """
    _attr_to_key: Dict[str, str] = dict(DictSubTreeData._attr_to_key)
    _attr_to_key["keys"] = "keys_key"

    keys_key = "keys"

    @dict_attribute
    def keys(self):
        pass

    @classmethod
    def get_sensor_paths(cls, content, current_path) -> Sequence[str]:
        """
        navigates the content, appending to the path.
        """
        if isinstance(content, list):
            for value in content:
                yield from cls.get_sensor_paths(value, current_path)
        if isinstance(content, dict):
            for elem, value in content.items():
                this_path = cls.form_encoding_path(current_path, [elem])
                yield this_path
                if isinstance(value, ContainersTypes):
                    yield from cls.get_sensor_paths(value, this_path)

    @property
    @lru_cache(maxsize=None)
    def sensor_paths(self) -> Sequence[str]:
        """
        Returns the sensor paths of all containers  of the element.
        """
        paths = list(self.get_sensor_paths(self.content, self.path))
        return paths

    def get_elements(self) -> "Sequence[DictElementData]":
        return [self]


class GrpcRaw(DictSubTreeData):
    """
    This is a general grpc metric, obtained through the grpc definitions from any vendor.
    Content is the grpc msg, it should be in bytes. The Metadata comes from the connection.
    """

    @classmethod
    def from_base64(cls, data: str, metadata=None) -> "GrpcRaw":
        if metadata is None:
            metadata = {}
        byte_data = base64.b64decode(data)
        metric_data = metadata.copy()
        metric_data["content"] = byte_data
        return cls(metric_data)
