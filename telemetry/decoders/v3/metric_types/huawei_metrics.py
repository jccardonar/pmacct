from metric_types.base_types import (
    DictSubTreeData,
    DictElementData,
    load_data_and_make_property,
    GrpcRaw,
    AmbiguousContent
)
import base64
import huawei_telemetry_pb2
from typing import Sequence
import importlib
from exceptions import PmgrpcdException

#from google.protobuf.json_format import MessageToDict
from proto_override import MessageToDictUint64 as MessageToDict, MessageToDictWithOptions
from base_transformation import BaseConverter, SimpleConversion, TransformationBase

def process_huawei_grpc_msg(msg, options=None):
    if options is None:
        options = {}
    telemetry_msg = huawei_telemetry_pb2.Telemetry()
    telemetry_msg.ParseFromString(msg)
    uint64_to_int = not options.get("proto_uint64_to_str", False)
    use_integers_for_enums = options.get("proto_enums_to_int", False)
    telemetry_msg_dict = MessageToDict(
        telemetry_msg,
        including_default_value_fields=True,
        preserving_proto_field_name=True,
        use_integers_for_enums=use_integers_for_enums,
        uint64_to_int=uint64_to_int,
    )
    return telemetry_msg_dict

class HuaweiElement(DictElementData):
    '''
    An element of the huawei tree.
    '''
    data_json_key = "data_json"
    data_gpb_key = "data_gpb"
    p_key = "sensor_path"
    subscription_id_key = "subscription_id_str"
    node_key = "node_id_str"
    timestamp_key = "msg_timestamp"

    @property
    def keys(self):
        raise NotImplementedError("Keys are not implemented for huwei elements")



class HuaweiDictSubTreeData(DictSubTreeData):
    '''
    Provides a base set of keys to extract elements from the huawei-telemetry.proto
    '''
    data_json_key = "data_json"
    data_gpb_key = "data_gpb"
    p_key = "sensor_path"
    subscription_id_key = "subscription_id_str"
    node_key = "node_id_str"
    timestamp_key = "msg_timestamp"

    @property
    def data_gpb(self):
        if self.data_gpb_key not in self.data:
            return None
        return self.data[self.data_gpb_key]

class HuaweiGrpcGPB(HuaweiDictSubTreeData):

    @property
    def content(self):
        raise AmbiguousContent("Huawei Raw Grpc does not have defined content")


class ErrorDecodingHuawei(PmgrpcdException):
    pass

class GrpcRawGPBToHuaweiGrpcGPB(BaseConverter):
    def __init__(self, proto_decoding_options=None, keep_headers=True):
        if proto_decoding_options is None:
            proto_decoding_options = {}
        self.proto_decoding_options = proto_decoding_options
        self.keep_headers = keep_headers

    def get_content(self, metric):
        try:
            decoded_content = process_huawei_grpc_msg(metric.content, self.proto_decoding_options)
        except Exception as e:

        return decoded_content

    def transform(self, metric: GrpcRaw, warnings=None):
        decoded_content = self.get_content(metric)
        if self.keep_headers:
            for header, value in metric.headers.items():
                if header not in decoded_content:
                    decoded_content[header] = value
        yield HuaweiGrpcGPB(decoded_content)


class HuaweiCompact(HuaweiDictSubTreeData, DictElementData):

    content_key = "data_gpb"

    def replace(self, content=None, keys=None, path=None):
        new_data = self.data.copy()
        if content is not None:
            new_data[self.content_key] = {"row": content}
        if keys is not None:
            new_data[self.keys_key] = keys
        if path is not None:
            new_data[self.p_key] = path
        return self.__class__(new_data)




    @property
    def content(self):
        if not self.data_gpb:
            return []
        return self.data_gpb.get("row", [])

    def get_elements(self)  -> Sequence[HuaweiElement]:
        huawei_elements = []
        for elem in self.content:
            # elem is a gpb message
            element_data = self.headers.copy()
            element_content = self.decoder.decode(elem)
            element_data[HuaweiElement.content_key] = element_content
            huawei_elements.append(HuaweiElement(element_data))

        return huawei_elements

class HuaweiGrpcGPBToHuaweiCompact(SimpleConversion):
    ORIGINAL_CLASS = HuaweiGrpcGPB
    RESULTING_CLASS = HuaweiCompact



class HuaweiDecoder:

    def __init__(self, msg_constructor, options=None):
        '''
        msg is a proto file
        '''
        if options is None:
            options = {}
        self.options = options
        self.msg_constructor = msg_constructor

    def decode(self, content):
        content_bytes = content
        if isinstance(content, str):
            # we assume b64 here
            content_bytes = base64.b64decode(content)
        msg = self.msg_constructor()
        msg.ParseFromString(content_bytes)
        # convert to dict
        dict_msg = MessageToDictWithOptions(msg, self.options)
        return dict_msg

class ModuleLoaderProblem(PmgrpcdException):
    pass

class HuaweDecoderConstructor:

    def __init__(self, proto_descriptor):
        '''
        proto_descriptor must be a dict[yang module]->[python module, msg]
        '''
        self.proto_descriptor = proto_descriptor

    def get_decoder(self, module):
        msg = self.get_msg(module)
        return HuaweiDecoder(msg)

    def get_msg(self, module):
        if module not in self.proto_descriptor:
            raise ModuleLoaderProblem(f"Module {module} is not defined")

        module_data = self.proto_descriptor[module]
        python_module_name = module_data["module"]
        msg_name = module_data["msg"]

        try:
            python_module = importlib.import_module(python_module_name)
        except ModuleNotFoundError:
            raise ModuleLoaderProblem(f"Module {module} cannot be found")
        try:
            msg = getattr(python_module, msg_name)
        except  AttributeError:
            raise ModuleLoaderProblem(f"Msg {msg_name} not found")

        return msg


class HuaweCompactToHuaweiElements(TransformationBase):
    def __init__(self, decoder):
        self.decoder = decoder

    def transform(self, metric):
        for elem in metric.content:
            # elem is a gpb message
            element_data = metric.headers.copy()
            element_data.pop("data_gpb", None)
            element_data.pop("data_json", None)
            element_content = self.decoder.decode(elem["content"])
            element_data[HuaweiElement.content_key] = element_content
            yield HuaweiElement(element_data)



