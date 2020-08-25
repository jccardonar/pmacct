"""
Metrics and transformations for NX.
NX supports multiple encodings/transport: GNMI, Cisco-GRPC, UDP.
NX has different APIs that can be configured. OpenConfig, show commands, and their API.
Experimentally, NX seems that NX only supports gpbkv when using the Cisco-GRPC.
Json is supported but on their UDP transport.
The NX API is modelled for XML encoding. The encoding is a bit particular. We do provide a
way of "pivoting" to the more typical relationships schema, but it is experimental and only tested
in a few paths.
Some links for reference:
    - https://developer.cisco.com/docs/nx-os/#!telemetry
    - https://developer.cisco.com/site/nxapi-dme-model-reference-api/
"""
from metric_types.cisco.cisco_metrics import (
    CiscoElement,
    CiscoGrpcGPB,
    CiscoGrpcKV,
    GrpcRawGPBToCiscoGrpcGPB,
    PivotingCiscoGPBKVDict,
)
from transformations.base_transformation import BaseConverter, SimpleConversion


class NxGrpcGPB(CiscoGrpcGPB):
    """
    Class for Cisco-GRPCs metrics. Similar to CiscoGrpcGPB, since it seems it
    is the only encoding they support.
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


# Transformation from  GrpcRaw to NxGrpcGPB
# ---


class GrpcRawToNxGrpcGPB(GrpcRawGPBToCiscoGrpcGPB):
    RESULTING_CLASS = NxGrpcGPB


# NXGrpcKV
# #####


class NXGrpcKV(CiscoGrpcKV):
    """
    Similar to CiscoGrpcKV but might contain NX specific functions.
    """

    def __init__(self, data, nx_api=False):
        super().__init__(data)


# Transformation from NxGrpcGPB to NXGrpcKV
# ---


class NxGrpcGPBToNXGrpcKV(SimpleConversion):
    ORIGINAL_CLASS = NxGrpcGPB
    RESULTING_CLASS = NXGrpcKV


# NX element
# #####


class NXElement(CiscoElement):
    """
    Equal to CiscoElement but might include some special functions for NX.
    """


# Transformation from CiscoElement to NXElement
# ---

class CiscoElementToNXElement(SimpleConversion):
    """
    Converts CiscoElements to NXElements.
    The Cisco GPBKV pivoting yields CiscoElement. These can then  be converted  to NXElement
    """

    ORIGINAL_CLASS = CiscoElement
    RESULTING_CLASS = NXElement

def nx_grpckv_to_nx_element(*args, **kargs):
    return PivotingCiscoGPBKVDict(element_class=NXElement, *args, **kargs)
