from enum import Enum
from abc import abstractmethod
from exceptions import PmgrpcdException

import ujson as json

import cisco_telemetry_pb2
from base_transformation import BaseConverter, SimpleConversion
from cisco_gbpvk_tools.cisco_gpbvkv import PivotingCiscoGPBKVDict
from metric_types.base_types import (
    AmbiguousContent,
    DictElementData,
    DictSubTreeData,
    GrpcRaw,
)
from proto_override import MessageToDictUint64 as MessageToDict


def process_cisco_grpc_msg(new_msg):
    """
    Processes a cisco grpc msg.
    Note that we are using the proto_override module which ignores the Uint64 conversion to str.
    TODO: Make this modular. It should also work with other proto implementations.
    """
    telemetry_msg = cisco_telemetry_pb2.Telemetry()
    telemetry_msg.ParseFromString(new_msg)
    # jsonStrTelemetry = MessageToJson(telemetry_msg)
    # grpc_message = json.loads(jsonStrTelemetry)
    grpc_message = MessageToDict(telemetry_msg, preserving_proto_field_name=True)
    return grpc_message


class EncodingNotFound(PmgrpcdException):
    pass


class CiscoEncodings(Enum):
    JSON = 0
    GPBVK = 1
    COMPACT = 2


class CiscoElement(DictElementData):
    """
    An element of a cisco subtree.
    """

    @property
    def subscription_id(self):
        '''
        A pity, but subscription id is a bit more complex.
        We will not able to replace subscription_id using the given method.
        '''
        options = ["subscription_id_str", "subscription_id"]
        for option in options:
            if option in self.data:
                return self.data[option]
        raise KeyError("No subscription found")

    @property
    def msg_timestamp(self):
        if self.msg_timestamp_key not in self.data:
            return self.collection_start_time
        return self.data[self.msg_timestamp_key]


class CiscoGrpcGPB(DictSubTreeData):
    """
    A general Cisco metric defined in the telemetry.proto from cisco.
    Content can be included in:
     - data (when it is encoded in another proto)
     - data_gpbkv (when it is encoded based in the self-contained msgs defined in the same proto)
     - data_json when it is a string encoded json
    """

    data_json_key = "data_json"
    data_gpbkv_key = "data_gpbkv"
    data_comp_key = "data"

    def infer_encoding(self) -> CiscoEncodings:
        """
        Infers the encoding based on where the data is located.
        """
        data_dict = {
            CiscoEncodings.JSON: self.data_json,
            CiscoEncodings.GPBVK: self.data_gpbkv,
            CiscoEncodings.COMPACT: self.data_comp,
        }
        non_empty_content = [x for x in data_dict.values() if x is not None]
        if len(non_empty_content) > 1:
            raise EncodingNotFound("Multiple values are not none")
        if not non_empty_content:
            raise EncodingNotFound("No data is set")
        return [x for x, y in data_dict.items() if y is not None][0]

    @property
    def data_json(self):
        if self.data_json_key not in self.data:
            return None
        return self.data[self.data_json_key]

    @property
    def data_gpbkv(self):
        if self.data_gpbkv_key not in self.data:
            return None
        return self.data[self.data_gpbkv_key]

    @property
    def data_comp(self):
        if self.data_comp_key not in self.data:
            return None
        return self.data[self.data_comp_key]

    @property
    def content(self):
        raise AmbiguousContent("Cisco Raw Grpc does not have defined content")

    @property
    def subscription_id(self):
        options = ["subscription_id_str", "subscription_id"]
        for option in options:
            if option in self.data:
                return self.data[option]
        raise KeyError("No subscription found")


class GrpcRawToCiscoGrpcGPB(BaseConverter):
    RESULTING_CLASS = CiscoGrpcGPB

    def __init__(self, keep_headers=True):
        self.keep_headers = keep_headers

    @abstractmethod
    def get_content(self, metric):
        """
        Decodes the content of the grpc msg.
        """
        raise NotImplementedError("Not implemented")

    def transform(self, metric: GrpcRaw, warnings=None):
        decoded_content = self.get_content(metric)
        if self.keep_headers:
            for header, value in metric.headers.items():
                if header not in decoded_content:
                    decoded_content[header] = value
        yield self.RESULTING_CLASS(decoded_content)


class GrpcRawJsonToCiscoGrpcGPB(GrpcRawToCiscoGrpcGPB):
    def get_content(self, metric):
        decoded_content = json.decode(metric.content)
        return decoded_content


class GrpcRawGPBToCiscoGrpcGPB(GrpcRawToCiscoGrpcGPB):
    def get_content(self, metric):
        decoded_content = process_cisco_grpc_msg(metric.content)
        return decoded_content


class CiscoGrpcJson(DictSubTreeData):
    content_key = "data_json"

    @property
    def subscription_id(self):
        options = ["subscription_id_str", "subscription_id"]
        for option in options:
            if option in self.data:
                return self.data[option]
        raise KeyError("No subscription found")

    @property
    def data_json(self):
        return self.data.get("data_json", None)

    @property
    def content(self):
        """
        We assume content is in data_gpbkv. This can change in later versions.
        """
        return self.data_json or []


class CiscoGrpcJsonToCiscoElement:
    def convert_json_content(self, content):
        return content

    @staticmethod
    def convert_json_keys(keys):
        new_keys = {}
        for key_value in keys:
            key, value = list(key_value.items())[0]
            PivotingCiscoGPBKVDict.add_to_flatten(new_keys, key, value)
        return new_keys

    def transform(self, metric):
        for element in metric.content:
            element_data = metric.headers.copy()
            keys = self.convert_json_keys(element.get("keys", []))
            content = self.convert_json_content(element.get("content", {}))

            # we will be igonring timestamp right now.
            element_data[CiscoElement.content_key] = content
            element_data[CiscoElement.keys_key] = keys
            yield CiscoElement(element_data)


class CiscoGrpcGPBToCiscoGrpcJson(SimpleConversion):
    ORIGINAL_CLASS = CiscoGrpcGPB
    RESULTING_CLASS = CiscoGrpcJson


class CiscoGrpcKV(DictSubTreeData):
    """
    Data is in python dict
    We pivot this form to get a CiscoSubTree
    """

    content_key = "data_gpbkv"
    element_class = CiscoElement

    @property
    def data_gpbkv(self):
        return self.data.get("data_gpbkv", None)

    @property
    def content(self):
        """
        We assume content is in data_gpbkv. This can change in later versions.
        """
        return self.data_gpbkv or []

    @property
    def subscription_id(self):
        options = ["subscription_id_str", "subscription_id"]
        for option in options:
            if option in self.data:
                return self.data[option]
        raise KeyError("No subscription found")


class CiscoGrpcGPBToCiscoGrpcKV(SimpleConversion):
    ORIGINAL_CLASS = CiscoGrpcGPB
    RESULTING_CLASS = CiscoGrpcKV


class NXElement(CiscoElement):
    """
    Equal to CiscoElement but some special functions for NX.
    """


class CiscoElementToNXElement(SimpleConversion):
    ORIGINAL_CLASS = CiscoElement
    RESULTING_CLASS = NXElement


class NxGrpcGPB(CiscoGrpcGPB):
    """
    TODO: fill the next with the actual documentation links and the right terminology.
    Experimentally, NX uses the same schema than XR (telemetry.proto), and it seems that only supports gpbkv.
    Json is supported but on their UDP transport.
    NX has different APIs that can be configured. OpenConfig, show commands, and their API.
    The NX API is modelled for XML encoding. The encoding is a bit particular. We do provide a
    way of "pivoting" to the more typical relationships schema, but it is experimental and only tested
    in a few paths.
    """

    content_key = "data_gpbkv"

    def infer_nx_path(self):
        raise NotImplementedError

    @property
    def content(self):
        """
        We assume content is in data_gpbkv. This can change in later versions.
        """
        return self.data_gpbkv or []


class GrpcRawToNxGrpcGPB(GrpcRawGPBToCiscoGrpcGPB):
    RESULTING_CLASS = NxGrpcGPB


class NXGrpcKV(CiscoGrpcKV):
    def __init__(self, data, nx_api=False):
        super().__init__(data)
        if nx_api:
            self.element_class = NXElement


class NxGrpcGPBToNXGrpcKV(SimpleConversion):
    ORIGINAL_CLASS = NxGrpcGPB
    RESULTING_CLASS = NXGrpcKV
