"""
Code to operate on metrics based on the messages defined or based in:
https://github.com/cisco/bigmuddy-network-telemetry-proto/tree/master/proto_archive/telemetry.proto (called telemetry.proto from here on).

Dial-in and dial-out grpc definitions are in https://github.com/cisco/bigmuddy-network-telemetry-proto/tree/master/proto_archive/mdt_grpc_dialin and mdt_grpc_dialout. We only support dial-out here (i.e. the server connects to the collector).

Both Dial-in and Dial-out define simple msgs with a data field that includes the payload. The payload can include a json string or an encoded telemetry.proto.

If the  encoding in json, the internal schema is (as far as tested) similar to the telemetry.proto, but with an extra data_json field. The headers of telemetry.proto (e.g. node_id, subscription, encoding_path, etc.) remain the same. The actual metric content (the values under the encoding path) are within the data_json key. Fortunatly, the content is not in a key-values schema (See data-gpbkv below).

If encoded in proto, the schema follows the telemetry.proto. The telemetry.proto includes 2 ways of encoding the actual data. Data "compact", where the data is stored on the data field, and the content is a byte string encoding the metric data. We dont currently support this mode.

The other way of encoding in the telemetry.proto is using the TelemetryField. This is a recursive message where content is stored in a key-value model (aka as a attribute-value pair or entity-value model: https://en.wikipedia.org/wiki/Entity%E2%80%93attribute%E2%80%93value_model). One navigates the recursive structure until finding one where "value" field is populated. We actually pivot this data to get something with a familiar structure that we can more simple process.
"""
from enum import Enum
from exceptions import PmgrpcdException
from base_transformation import BaseConverter, SimpleConversion
from abc import abstractmethod

import ujson as json

from metric_types import AmbiguousContent, DictElementData, DictSubTreeData, GrpcRaw
from metric_types.cisco.cisco_transformations_functions import process_cisco_grpc_msg
from .cisco_gpbvkv import PivotingCiscoGPBKVDict


class EncodingNotFound(PmgrpcdException):
    pass


class CiscoEncodings(Enum):
    JSON = 0
    GPBVK = 1
    COMPACT = 2


class CiscoMixin:
    "A series of functions common across all cisco metrics."

    @property
    def subscription_id(self):
        """
        A pity, but subscription id is a bit more complex.
        We will not able to replace subscription_id using the given method.
        """
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


# CiscoGrpcGPB
# #####


class CiscoGrpcGPB(CiscoMixin, DictSubTreeData):
    """
    A general metric defined in the telemetry.proto from cisco.
    Content can be included in:
     - data (when it is encoded in another proto)
     - data_gpbkv (when it is encoded based in the self-contained msgs defined in the same proto)
     - data_json when it is a string encoded json.
     Obtained from a MdtDialoutArgs msg obtained from the cisco dial-out grpc.
     To do that, we need to decode the MdtDialoutArgs.data.
     If this data is encoded in json, we decode json.
     If the data is encoded in GPB, we decode gpb.
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


# Transformation from a GrpcRaw to CiscoGrpcGPB
# ----
# Since this depends on the encoding, there are multiple transformations.


class GrpcRawToCiscoGrpcGPB(BaseConverter):
    """
    Parent class generalizing the process of converting the GrpcRaw to a CiscoGrpcGPB
    There are two ways, depending on the encoding.
    Note that the mapping of Cisco MdtDialoutArgs to a GrpcRaw.content
    actually includes the content of the MdtDialoutArgs.data, not the whole
    MdtDialoutArgs msg.  That was actually a mistake and we will fix it at
    some point.
    """

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
    """
    Converting a json encoded packet into a CiscoGrpcGPB
    """

    def get_content(self, metric):
        decoded_content = json.decode(metric.content)
        return decoded_content


class GrpcRawGPBToCiscoGrpcGPB(GrpcRawToCiscoGrpcGPB):
    """
    Converting a GPB  encoded packet into a CiscoGrpcGPB.
    Uses the telemetry.proto for decoding the string.
    """

    def get_content(self, metric):
        decoded_content = process_cisco_grpc_msg(metric.content)
        return decoded_content


# CiscoGrpcJson
# #######


class CiscoGrpcJson(CiscoMixin, DictSubTreeData):
    """
    A Cisco json message. Basically the same content as data
    as CiscoGrpcGPB, but knowing where the data is data_json. 
    The data is the same since when decoding the json msg, the 
    content gets decoded as well.
    """

    content_key = "data_json"

    @property
    def data_json(self):
        return self.data.get("data_json", None)

    @property
    def content(self):
        """
        We assume content is in data_gpbkv. This can change in later versions.
        """
        return self.data_json or []


# Transformation from CiscoGrpcGPB to CiscoGrpcJson
# -----


class CiscoGrpcGPBToCiscoGrpcJson(SimpleConversion):
    """
    Transformation between CiscoGrpcGPB and CiscoGrpcJson is simple: just passing the data
    """

    ORIGINAL_CLASS = CiscoGrpcGPB
    RESULTING_CLASS = CiscoGrpcJson


# CiscoGrpcKV (Key value) metric
# #######


class CiscoGrpcKV(CiscoMixin, DictSubTreeData):
    """
    Same structure as a  CiscoGrpcGPB but knowing that the data is in data_gpbkv.
    We do not actually process KV value data. We pivot it later  so that 
    we can process it.
    """

    content_key = "data_gpbkv"

    @property
    def data_gpbkv(self):
        return self.data.get("data_gpbkv", None)

    @property
    def content(self):
        """
        We assume content is in data_gpbkv. This can change in later versions.
        """
        return self.data_gpbkv or []


# Transformation from CiscoGrpcGPB to CiscoGrpcKV
# -----


class CiscoGrpcGPBToCiscoGrpcKV(SimpleConversion):
    """
    Transforamtion from CiscoGrpcGPB to CiscoGrpcKV is simple (sort of a casting)
    """

    ORIGINAL_CLASS = CiscoGrpcGPB
    RESULTING_CLASS = CiscoGrpcKV


# CiscoElement
# ########


class CiscoElement(CiscoMixin, DictElementData):
    """
    An element of a cisco subtree.
    In the "previous" metrics, the payload included data for multiple "keys" (e.g. Multiple interfaces).
    A CiscoElement should contain data from only one entity.
    Data should alsobe contained in a simple python dictionary and in a schema-full model, that is:
        - no {"name": "interface", "value": 1}, but: {"interface": 1}
    """


# Transformations to CiscoElement
# -----


class CiscoGrpcJsonToCiscoElement:
    """
    Converts  CiscoGrpcJson to CiscoElement
    Cisco-json does not have a KV schema, but they split elements in the first "hierarchy" 
    of the content. The first element should be a list, each one with a dictionary
    where "keys" is a container with the keys, and "content" has the rest of the data.
    This code splits them
    """

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


# Transformations from KV to Cisco elements are in module cisco_gpbvkv
