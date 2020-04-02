# Imitates the generation of cisco telemetry.
# Use for testing.
import cisco_grpc_dialout_pb2_grpc
import grpc

# Dialout code
# Simple for now, it could get complicated if we need a more time based example.
class CiscoDialOutClient():
    def __init__(self, server):
        self.server = server
        self.channel = grpc.insecure_channel(self.server)
        self.stub = cisco_grpc_dialout_pb2_grpc.gRPCMdtDialoutStub(self.channel)

    def send_data(self, data):
        self.rcv  = self.stub.MdtDialout(data)


    def close(self):
        self.channel.close()


