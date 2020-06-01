from metric_types.base_types import (
    DictSubTreeData,
    DictElementData,
    load_data_and_make_property,
    GrpcRaw,
)
from cisco_gbpvk_tools.cisco_gpbvkv import PivotingCiscoGPBKVDict
from nx_tools.nx_api import PivotingNXApiDict
from enum import Enum
from typing import Sequence
import ujson as json

import cisco_telemetry_pb2
from google.protobuf.json_format import MessageToDict
from exceptions import PmgrpcdException


def process_cisco_kv(new_msg):
    """
    Processes a cisco grpc msg.
    TODO: Make this modular. It should also work with other 
    proto implementations.
    """
    telemetry_msg = cisco_telemetry_pb2.Telemetry()
    telemetry_msg.ParseFromString(new_msg)
    # jsonStrTelemetry = MessageToJson(telemetry_msg)
    # grpc_message = json.loads(jsonStrTelemetry)
    grpc_message = MessageToDict(telemetry_msg, preserving_proto_field_name=True)
    return grpc_message


class EncodingNotFound(PmgrpcdException):
    pass


class AmbiguousContent(PmgrpcdException):
    pass


class CiscoEncodings(Enum):
    JSON = 0
    GPBVK = 1
    COMPACT = 2


class CiscoSubTree:
    """
    A subtree of cisco in python dictionary form.
    In an instance of this type, data should similar to what a yang representation should b (e.g. relational model, no open schemas, no EAV, no "keys", "values", "childrens" fields)
    """

    def get_elements(self) -> Sequence["CiscoElement"]:
        """
        Returns a sequence of elements.
        """


class CiscoElement(DictElementData):
    """
    An element of a cisco subtree.
    Again, following a relational model
    """

    def get_elements(self) -> "Sequence[CiscoElement]":
        return [self]


class CiscoGrpcGPB(DictSubTreeData):
    """
    This is a cisco general grpc based on the telemetry grpc in:
    """

    data_json_key = "data_json"
    data_gpvkv_key = "data_gpbkv"
    data_comp_key = "data"

    def infer_encoding(self) -> CiscoEncodings:
        """
        Infers the encoding based on where the data is located.
        """
        data_dict = {
            CiscoEncodings.JSON: self.data_json,
            CiscoEncodings.GPBVK: self.data_gpvkv,
            CiscoEncodings.COMPACT: self.data_comp,
        }
        if len([x for x in data_dict.values() if x is not None]) > 1:
            raise EncodingNotFound("Multiple values are not none")
        if len([x for x in data_dict.values() if x is not None]) == 0:
            raise EncodingNotFound("No data is set")
        return [x for x, y in data_dict.items() if y is not None][0]

    @property
    def data_json(self):
        if self.data_json_key not in self.data:
            return None
        return self.data[self.data_json_key]

    @property
    def data_gpvkv(self):
        if self.data_gpvkv_key not in self.data:
            return None
        return self.data[self.data_gpvkv_key]

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

    @classmethod
    def from_grpc_msg_gpb(
        cls, metric_grpc_raw: GrpcRaw, keep_headers=True
    ) -> "CiscoGrpcGPB":
        """
        Constructs a CiscoGrpcGPB from a GrpcRaw.
        It needs to decode the raw grpc msg using the cisco proto.
        """
        decoded_content = process_cisco_kv(metric_grpc_raw.content)
        if keep_headers:
            for header, value in metric_grpc_raw.headers.items():

                if header not in decoded_content:
                    decoded_content[header] = value
        return cls(decoded_content)

    @classmethod
    def from_grpc_msg_json(
        cls, metric_grpc_raw: GrpcRaw, keep_headers=True
    ) -> "CiscoGrpcGPB":
        """
        Constructs a CiscoGrpcGPB from a GrpcRaw.
        It needs to decode the raw grpc msg using the cisco proto.
        """
        decoded_content = json.decode(metric_grpc_raw.content)
        if keep_headers:
            for header, value in metric_grpc_raw.headers.items():

                if header not in decoded_content:
                    decoded_content[header] = value
        return cls(decoded_content)


class CiscoGrpcJson(DictSubTreeData):
    content_key = "data_json"

    @classmethod
    def from_cisco_grpc_gpb(
        cls, metric_cisco_grpc_gpb: CiscoGrpcGPB, keep_headers=True
    ) -> "CiscoGrpcJson":
        """
        Constructs a CiscoGrpcKV using a CiscoGrpcGPB. 
        """
        data = metric_cisco_grpc_gpb.data.copy()
        # Have empty content, if not available.
        if cls.content_key not in data:
            data[cls.content_key] = []
        return cls(data)

    def convert_json_keys(self, keys):
        new_keys = {}
        for key_value in keys:
            key, value = list(key_value.items())[0]
            PivotingCiscoGPBKVDict.add_to_flatten(new_keys, key, value)
        return new_keys

    def convert_json_content(self, content):
        return content

    def get_elements(self) -> Sequence[CiscoElement]:
        cisco_elements = []
        for element in self.content:
            element_data = self.headers.copy()
            keys = self.convert_json_keys(element.get("keys", []))
            content = self.convert_json_content(element.get("content", {}))

            # we will be igonring timestamp right now.
            element_data[CiscoElement.content_key] = content
            element_data[CiscoElement.keys_key] = keys
            cisco_elements.append(CiscoElement(element_data))
        return cisco_elements

    @property
    def subscription_id(self):
        options = ["subscription_id_str", "subscription_id"]
        for option in options:
            if option in self.data:
                return self.data[option]
        raise KeyError("No subscription found")


class CiscoGrpcKV(DictSubTreeData):
    """
    Data is in python dict 
    We pivot this form to get a CiscoSubTree
    """

    content_key = "data_gpbkv"
    element_class = CiscoElement

    def __init__(self, data, pivoter=None):
        self.pivoter = pivoter
        super().__init__(data)

    @classmethod
    def from_cisco_grpc_gpb(
        cls, metric_cisco_grpc_gpb: CiscoGrpcGPB, keep_headers=True
    ) -> "CiscoGrpcKV":
        """
        Constructs a CiscoGrpcKV using a CiscoGrpcGPB. 
        """
        data = metric_cisco_grpc_gpb.data.copy()
        # Have empty content, if not available.
        if cls.content_key not in data:
            data[cls.content_key] = []
        return cls(data)

    def get_elements(self) -> Sequence[CiscoElement]:
        return self.pivot_data(self)

    def pivot_data(self) -> Sequence[CiscoElement]:
        """
        Pivots the data and returns a CiscoSubTree
        """
        pivoter = self.pivoter
        if pivoter is None:
            pivoter = PivotingCiscoGPBKVDict()
        pivoted_elements = pivoter.pivot_telemetry_fields(self.content)

        cisco_elements = []
        for element_content in pivoted_elements:
            element_data = self.headers.copy()
            # we will be igonring timestamp right now.
            element_data[self.element_class.content_key] = element_content["content"]
            element_data[self.element_class.keys_key] = element_content["keys"]
            cisco_elements.append(self.element_class(element_data))
        return cisco_elements

    @property
    def subscription_id(self):
        options = ["subscription_id_str", "subscription_id"]
        for option in options:
            if option in self.data:
                return self.data[option]
        raise KeyError("No subscription found")


class NxAPIDataType(Enum):
    OPENCONFIG = 0
    API = 1
    SHOW = 2


class NXElement(CiscoElement):
    """
    Equal to CiscoElement but some special functions for NX.
    """

    def get_elements(self):
        """
        Transforms the data into a format more like the internal representation. Removing children and attributes trees and making them all fields.
        """
        # Make sure we do a list, in case we find that at some point
        content = self.content
        if not isinstance(content, list):
            content = [content]

        for sample in content:
            new_content = PivotingNXApiDict().convert_nx_api(sample)
            data = self.data.copy()
            data[self.content_key] = new_content
            yield CiscoElement(data)


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

    # using the next, the resulting elemens from get_elements should be  NXElement.
    element_class = NXElement

    def infer_nx_path(self):
        raise NotImplementedError

    @property
    def content(self):
        """
        We assume content is in data_gpvkv. This can change in later versions.
        """
        return self.data_gpvkv or []


class NXGrpcKV(CiscoGrpcKV):
    pass
