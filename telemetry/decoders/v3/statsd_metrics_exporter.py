# All of this is based on https://github.com/jsocol/pystatsd, but adding
# formatting to fit the statsd-prometheus gateway CLibarto format)
# Does not support sample rate yet.
import socket
import time
import traceback as tb
from typing import Any, Dict

class LibratoFormatter:

    def format(self, name, labels):
        labels_str = ','.join(f'{x}={y}' for x,y in labels.items())
        return f"{name}#{labels_str}"

class StatsDMetricExporter:
    def __init__(self, host="localhost", port=8142, prefix=None, formatter=None):
        self.host = host
        self.port = int(port)
        self.addr = (socket.gethostbyname(self.host), self.port)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.prefix = prefix
        if formatter is None:
            formatter = LibratoFormatter()
        self.formatter = formatter

    def incr(self, name, labels):
        metric = self.build_counter(name, 1, labels)
        self.send(metric)


    def build_counter(self, name: str, value: Any, labels: Dict[str, Any]):
        new_name = name
        if self.prefix:
            new_name = '.'.join([self.prefix, new_name])
        metric_name = self.formatter.format(new_name, labels)
        full_metric = f"{metric_name}:{value}|c"
        return full_metric

    def send(self, packet):
        self.udp_sock.sendto(bytes(bytearray(packet, "utf-8")), self.addr)


if __name__ == "__main__":
    host = "127.0.0.1"
    port = 9125
    client = StatsDMetricExporter(host, port)
    client.incr("test", {"value": 1, "value2": 2})
    client.incr("test_two", {})
