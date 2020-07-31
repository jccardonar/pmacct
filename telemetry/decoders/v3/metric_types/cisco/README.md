# Cisco metrics and transformations

This module includes the definition of cisco's metrics and transformations between them.

Cisco metrics are the ones based on cisco's grpc calls and its telemetry proto:

We focus on Cisco's dial-out grpc call:
https://github.com/cisco/bigmuddy-network-telemetry-proto/tree/master/proto_archive/mdt_grpc_dialout

Which includes messages with the cisco's telemetry.proto schema:
https://github.com/cisco/bigmuddy-network-telemetry-proto/blob/master/proto_archive/telemetry.proto

History will tell whether cisco keeps using this definition, or decides to continue developing GNMI, or any future tech (e.g. YANG PUSH).

We consider a single proto for all OS versions. There might be nuances across vendors. For instances, NX takes some liberty on mapping some of the data it streams to the cisco messages.
