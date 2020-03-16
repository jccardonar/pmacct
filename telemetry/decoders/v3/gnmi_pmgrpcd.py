"""
Implements a simple gNMI client. Still no fancy features here, like:
    - Evaluting duplicates to detect slow consumption
    - Capabilites. 
    - Related to Capabilities: etecting if the paths are supported by target (since it seems that some targets simply do not send anything and do not complain about an unsupported path)
The specifications for gnmi can be found in https://github.com/openconfig/gnmi
Although the gnmi standard is quite detailed, it was very nice to see python examples of the interface from https://github.com/nokia/pygnmi
"""
import gnmi_pb2
import gnmi_pb2_grpc
import lib_pmgrpcd



class GNMIClient:

    def __init__(self, channel):
        self.channel = channel
        self.stub = gnmi_pb2_grpc.gNMIStub(self.channel)
        # ask for the capabilites
        cap_req = gnmi_pb2.CapabilityRequest()
        cap_res = self.stub.Capabilities(cap_req)
        breakpoint()




