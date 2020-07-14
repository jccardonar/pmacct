from gnmi_pb2_grpc import gNMIServicer, add_gNMIServicer_to_server
import gnmi_pb2
from utils import generate_content_from_raw
import grpc
import time
from concurrent import futures

from optparse import OptionParser

DEFAULT_CONNECTION = "127.0.0.1:6000"

parser = OptionParser()
parser.add_option(
    "-f",
    "--file",
    dest="file",
    help="File with raw data",
)
parser.add_option(
    "-c",
    "--connection",
    default=str(DEFAULT_CONNECTION),
    help="IP (socket address) of the collector",
)

(options, _) = parser.parse_args()

# Let us read the file and pack it in subscriptions.
responses = []
for line in generate_content_from_raw(options.file):
    response = gnmi_pb2.SubscribeResponse()
    response.ParseFromString(line)
    responses.append(response)



class GnmiServicerGenerator(gNMIServicer):

    def __init__(self, responses):
        self.responses = responses

    def Capabilities(self, request, context):
        print("Got a capability request from: ", context)
        cap_reply = gnmi_pb2.CapabilityResponse()
        return cap_reply

    def Get(self, request, context):
        pass

    def Subscribe(self, request, context):
        print("Got a Subscribe request from: ", context)
        for subs_res in self.responses:
            yield subs_res


gRPCserver = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2),
    )

add_gNMIServicer_to_server(
    GnmiServicerGenerator(responses), gRPCserver
)

print("Starting GNMI with options:", options)
gRPCserver.add_insecure_port(options.connection)
gRPCserver.start()

while True:
    time.sleep(5)

