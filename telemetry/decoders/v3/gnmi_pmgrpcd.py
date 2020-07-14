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
import os
from typing import Optional, Tuple, Sequence
from gnmi_utils import parse_gnmi_metadata
from lib_pmgrpcd import PMGRPCDLOG
from gnmi_utils import simple_gnmi_string_parser
import base64

def get_metadata() -> Optional[str]:
    '''
    This is a function with side effects, since return a value from env.
    Meaning that changing the value of the env in the middle of the
    execution could change behaviour. This should be no problem
    when performing the GRPC calls just at the start of some program, though.

    Since metadata might include credentials, I prefer 
    to have this than to store them.
    '''
    if lib_pmgrpcd.OPTIONS.gnmi_metadata:
        return lib_pmgrpcd.OPTIONS.gnmi_metadata
    if lib_pmgrpcd.OPTIONS.gnmi_metadata_env:
        if lib_pmgrpcd.OPTIONS.gnmi_metadata_env not in os.environ:
            raise Exception(f"GNMI metadata env set ({lib_pmgrpcd.OPTIONS.gnmi_metadata_env}), but not present in environment")
        return os.environ[lib_pmgrpcd.OPTIONS.gnmi_metadata_env]

class GNMIClient:

    def __init__(self, channel, xpaths: Sequence[str], sample_interval,  add_metadata=True):
        '''
        add_metadata: Adds metadata to calls (obtained via the get_metadata method)
        '''
        self.channel = channel
        self.stub = gnmi_pb2_grpc.gNMIStub(self.channel)
        self.add_metadata = add_metadata
        # let us convert the xpaths to GNMI paths.
        self.xpaths = xpaths
        self._capabilites = None
        self.paths = self.process_xpaths(xpaths)
        self.sample_interval = sample_interval

    def __metadata(self) -> Optional[Sequence[Tuple[str, str]]]:
        metadata_str = get_metadata()
        if metadata_str:
            metadata = parse_gnmi_metadata(metadata_str)
            return metadata
    
    @staticmethod
    def process_xpaths(xpaths):
        paths = []
        for xpath in xpaths:
            path = simple_gnmi_string_parser(xpath)
            paths.append(path)
        return tuple(paths)


    def non_supported_modules(self) -> Sequence[str]:
        '''
        If capabilities are supported by the gnmi server, 
        this returns the list of modules not supported.
        '''
        pass

    def capabilities(self):
        if self._capabilites:
            return self._capabilites
        self._capabilites = self._get_capabilities()

    def _get_capabilities(self):
        metadata = None
        if self.add_metadata:
            metadata = self.__metadata()
        breakpoint()
        cap_req = gnmi_pb2.CapabilityRequest()
        cap_res = self.stub.Capabilities(cap_req, metadata=metadata)
        return cap_res

    def get_subscriptions(self):
        '''
        Converts provided paths to subscriptions. 
        '''
        subscriptions = []
        for path in self.paths:
            subscription = gnmi_pb2.Subscription()
            subscription.path.CopyFrom(path)
            subscription.mode = gnmi_pb2.SAMPLE
            subscription.sample_interval = self.sample_interval
            subscriptions.append(subscription)
        return subscriptions


    def subscribe(self):
        subs_req = gnmi_pb2.SubscribeRequest()
        subscribe_req = subs_req.subscribe
        subscriptins = self.get_subscriptions()
        subscribe_req.subscription.extend(subscriptins)
        subscribe_req.encoding = gnmi_pb2.PROTO

        metadata = None
        if self.add_metadata:
            metadata = self.__metadata()
        stream = self.stub.Subscribe((x for x in [subs_req]), metadata=metadata)
        PMGRPCDLOG.debug("GNMI subscription requested.")
        for element in stream:
            PMGRPCDLOG.trace("GNMI subscription received")

            # dump the raw data
            if lib_pmgrpcd.OPTIONS.rawdatadumpfile:
                PMGRPCDLOG.trace("Write rawdatadumpfile: %s", lib_pmgrpcd.OPTIONS.rawdatadumpfile)
                with open(lib_pmgrpcd.OPTIONS.rawdatadumpfile, "a") as rawdatafile:
                    rawdatafile.write(base64.b64encode(element.SerializeToString()).decode())
                    rawdatafile.write("\n")


