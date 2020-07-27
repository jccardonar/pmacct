Metrics and basics transformations between them for cisco metrics.

Cisco metrics are the ones based on cisco's telemetry proto.
https://github.com/cisco/bigmuddy-network-telemetry-proto/tree/master/proto_archive

History will tell whether cisco keeps using this definition, or decides to continue developing GNMI, or any future tech (e.g. YANG PUSH).

We consider a single proto for all OS versions. There might be nuances across vendors. For instances, NX takes some liberty on mapping some of the data it streams to the cisco messages.

