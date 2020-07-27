from base_transformation import BaseConverter, SimpleConversion
from metric_types.cisco.cisco_metrics import CiscoGrpcGPB, GrpcRaw, CiscoGrpcJson, CiscoGrpcKV
import ujson as json
from proto_override import MessageToDictUint64 as MessageToDict
import cisco_telemetry_pb2
from cisco_gbpvk_tools.cisco_gpbvkv import PivotingCiscoGPBKVDict
from abc import abstractmethod

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



class CiscoElementToNXElement(SimpleConversion):
    ORIGINAL_CLASS = CiscoElement
    RESULTING_CLASS = NXElement


class GrpcRawToNxGrpcGPB(GrpcRawGPBToCiscoGrpcGPB):
    RESULTING_CLASS = NxGrpcGPB



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


