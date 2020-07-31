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



