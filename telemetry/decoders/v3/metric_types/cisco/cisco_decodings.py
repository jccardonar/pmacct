"""
This file includes basic "recipes" for decoding differnet metrics.
Helps upper level modules to ignore many of the cisco transformations 
"""
from .cisco_metrics import (
    GrpcRawJsonToCiscoGrpcGPB,
    CiscoGrpcGPBToCiscoGrpcJson,
    CiscoGrpcJsonToCiscoElement,
    GrpcRawGPBToCiscoGrpcGPB,
    CiscoGrpcGPBToCiscoGrpcKV,
    cisco_grpc_gpbkv_to_cisco_element
)
from .nx import GrpcRawToNxGrpcGPB, NxGrpcGPBToNXGrpcKV, nx_grpckv_to_nx_element
from .nx_api import PivotingNXApiDict


def decode_raw_json(raw_metric):
    cisco_grpc_gpb_metric = GrpcRawJsonToCiscoGrpcGPB().convert(raw_metric)
    cisco_json = CiscoGrpcGPBToCiscoGrpcJson().convert(cisco_grpc_gpb_metric)
    element_decoder = CiscoGrpcJsonToCiscoElement()
    return cisco_json, element_decoder


def decode_raw_gpvkv(raw_metric):
    cisco_grpc_gpb_metric = GrpcRawGPBToCiscoGrpcGPB().convert(raw_metric)
    cisco_gpbkv = CiscoGrpcGPBToCiscoGrpcKV().convert(cisco_grpc_gpb_metric)
    element_decoder = cisco_grpc_gpbkv_to_cisco_element()
    return cisco_gpbkv, element_decoder


def decode_raw_nx(raw_metric):
    cisco_grpc_gpb_metric = GrpcRawToNxGrpcGPB().convert(raw_metric)
    cisco_nx_gpbkv = NxGrpcGPBToNXGrpcKV().convert(cisco_grpc_gpb_metric)
    element_decoder = cisco_grpc_gpbkv_to_cisco_element()
    return cisco_nx_gpbkv, element_decoder


def json_pipeline():
    return [
        GrpcRawJsonToCiscoGrpcGPB(),
        CiscoGrpcGPBToCiscoGrpcJson(),
        CiscoGrpcJsonToCiscoElement(),
    ]


def gpvkv_pipeline():
    return [GrpcRawGPBToCiscoGrpcGPB(), CiscoGrpcGPBToCiscoGrpcKV(), cisco_grpc_gpbkv_to_cisco_element()]

def nx_gpvk_pipeline():
    return [
        GrpcRawToNxGrpcGPB(),
        NxGrpcGPBToNXGrpcKV(),
        nx_grpckv_to_nx_element()]

def nx_api_pipeline():
    nx_gpvk = nx_gpvk_pipeline()
    nx_gpvk.append(PivotingNXApiDict())
    return nx_gpvk

