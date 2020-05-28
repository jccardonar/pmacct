#
#   pmacct (Promiscuous mode IP Accounting package)
#   pmacct is Copyright (C) 2003-2019 by Paolo Lucente
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#   pmgrpcd and its components are Copyright (C) 2018-2019 by:
#
#   Matthias Arnold <matthias.arnold@swisscom.com>
#   Juan Camilo Cardona <jccardona82@gmail.com>
#   Thomas Graf <thomas.graf@swisscom.com>
#   Paolo Lucente <paolo@pmacct.net>
#
from lib_pmgrpcd import PMGRPCDLOG
import cisco_grpc_dialout_pb2_grpc
from google.protobuf.json_format import MessageToDict
import ujson as json
import lib_pmgrpcd
import time
from export_pmgrpcd import FinalizeTelemetryData
import base64
from debug import get_lock

import cisco_telemetry_pb2


def process_cisco_kv(new_msg):
    """
    Processes a msg using gpb-kv
    """
    telemetry_msg = cisco_telemetry_pb2.Telemetry()
    telemetry_msg.ParseFromString(new_msg.data)
    # jsonStrTelemetry = MessageToJson(telemetry_msg)
    # grpc_message = json.loads(jsonStrTelemetry)
    grpc_message = MessageToDict(telemetry_msg, preserving_proto_field_name=True)
    return grpc_message


from types import FunctionType, GeneratorType
import collections
from functools import wraps
import inspect


def log_wrapper(method):
    print(f"Patching {method}")

    @wraps(method)
    def wrapped(*args, **kwargs):
        try:
            yield from method(*args, **kwargs)
        except Exception as e:
            import logging

            logger = logging.getLogger("PMGRPCDLOG")
            logger.exception("Error performing method")
            raise

    return wrapped


class ServicerMiddlewareClass(type):
    def __new__(meta, classname, bases, class_dict):
        new_class_dict = {}

        for attribute_name, attribute in class_dict.items():
            if inspect.isgeneratorfunction(attribute):
                # replace it with a wrapped version
                attribute = log_wrapper(attribute)

            new_class_dict[attribute_name] = attribute

        return type.__new__(meta, classname, bases, new_class_dict)


class gRPCMdtDialoutServicer(
    cisco_grpc_dialout_pb2_grpc.gRPCMdtDialoutServicer,
    metaclass=ServicerMiddlewareClass,
):
    def __init__(self):
        PMGRPCDLOG.info("Cisco: Initializing gRPCMdtDialoutServicer()")

    def MdtDialout(self, msg_iterator, context):
        # breakpoint() if DEBUG_LOCK.acquire() else None

        # Get information about the peer, and print it.
        grpcPeer = {}
        grpcPeerStr = context.peer()
        (
            grpcPeer["telemetry_proto"],
            grpcPeer["telemetry_node"],
            grpcPeer["telemetry_node_port"],
        ) = grpcPeerStr.split(":")
        grpcPeer["ne_vendor"] = "Cisco"
        PMGRPCDLOG.debug("Cisco MdtDialout Message: %s" % grpcPeer["telemetry_node"])

        metadata = dict(context.invocation_metadata())
        grpcPeer["user-agent"] = metadata["user-agent"]
        # Example of grpcPeerStr -> 'ipv4:10.215.133.23:57775'
        grpcPeer["grpc_processing"] = "cisco_grpc_dialout_pb2_grpc"
        grpcPeer["grpc_ulayer"] = "GPB Telemetry"
        jsonTelemetryNode = json.dumps(grpcPeer, indent=2, sort_keys=True)

        PMGRPCDLOG.debug("Cisco connection info: %s" % jsonTelemetryNode)

        # Now go over the msgs
        for new_msg in msg_iterator:
            # breakpoint() if get_lock() else None
            PMGRPCDLOG.trace("Cisco new_msg iteration message")

            # TODO:
            # Should this be placed into an interceptor? why per msg and not per connection?
            # filter msgs that do not match the IP option if enabled.
            if lib_pmgrpcd.OPTIONS.ip:
                if grpcPeer["telemetry_node"] != lib_pmgrpcd.OPTIONS.ip:
                    continue
                PMGRPCDLOG.debug(
                    "Cisco: ip filter matched with ip %s" % (lib_pmgrpcd.OPTIONS.ip)
                )

            try:
                cisco_processing(grpcPeer, new_msg)
            except Exception as e:
                PMGRPCDLOG.debug("Error processing Cisco packet, error is %s", e)
                continue
        return
        yield


def cisco_processing(grpcPeer, new_msg):
    messages = {}
    grpc_message = {}
    encoding_type = None
    PMGRPCDLOG.trace("Cisco: Received GRPC-Data")
    # this is too much
    # PMGRPCDLOG.debug(new_msg.data)

    # dump the raw data
    if lib_pmgrpcd.OPTIONS.rawdatadumpfile:
        PMGRPCDLOG.trace("Write rawdatadumpfile: %s", lib_pmgrpcd.OPTIONS.rawdatadumpfile)
        with open(lib_pmgrpcd.OPTIONS.rawdatadumpfile, "a") as rawdatafile:
            rawdatafile.write(base64.b64encode(new_msg.data).decode())
            rawdatafile.write("\n")

    # Find the encoding of the packet
    try:
        encoding_type, grpc_message = find_encoding_and_decode(new_msg)
    except Exception as e:
        PMGRPCDLOG.error("Error decoding packet. Error is {}".format(e))
        raise

    PMGRPCDLOG.trace("encoding_type is: %s\n" % (encoding_type))

    if (encoding_type == "unknown") or encoding_type is None:
        raise Exception("Encoding type unknown")

    message_header_dict = {x:y for x,y in grpc_message.items() if "data" not in x}

    PMGRPCDLOG.trace("Header:%s", message_header_dict)

    # we collect data from the peer for logging.
    (node_ip) = grpcPeer["telemetry_node"]
    (ne_vendor) = grpcPeer["ne_vendor"]
    epochmillis = int(round(time.time() * 1000))

    full_ecoding_path = message_header_dict["encoding_path"]

    if ":" in full_ecoding_path:
        (proto, path) = message_header_dict["encoding_path"].split(":")
    else:
        proto = None
        path = full_ecoding_path

    (node_id_str) = message_header_dict["node_id_str"]
    message_header_dict.update({"encoding_type": encoding_type})

    if encoding_type == "ciscojson":
        elem = len(grpc_message["data_json"])
        messages = grpc_message["data_json"]
    elif encoding_type == "ciscogrpckv":
        if "dataGpbkv" in grpc_message:
            elem = len(grpc_message["dataGpbkv"])
            messages = grpc_message["dataGpbkv"]
        else:
            elem = 0
            messages = {}

    message_header_dict["path"] = path

    PMGRPCDLOG.trace(
        "EPOCH=%-10s NIP=%-15s NID=%-20s VEN=%-7s PT=%-22s ET=%-12s ELEM=%s",
        epochmillis,
        node_ip,
        node_id_str,
        ne_vendor,
        proto,
        encoding_type,
        elem,
    )

    # A single telemetry packet can contain multiple msgs (each having their own key/values).
    # here we are processing them one by one.
    for listelem in messages:
        # Copy the necessary metadata to the packet.
        PMGRPCDLOG.trace("LISTELEM: %s", listelem)

        message_dict = {}
        message_dict.update({"collector": {"grpc": {}}})
        message_dict["collector"]["grpc"].update(
            {"grpcPeer": grpcPeer["telemetry_node"]}
        )
        message_dict["collector"]["grpc"].update({"ne_vendor": grpcPeer["ne_vendor"]})
        message_dict["collector"].update({"data": message_header_dict})

        if encoding_type == "ciscojson":
            PMGRPCDLOG.trace("TEST: %s | %s", path, listelem["content"])
            message_dict.update({path: listelem["content"]})
        elif encoding_type == "ciscogrpckv":
            PMGRPCDLOG.trace("TEST: %s | %s", path, listelem["fields"])
            message_dict.update({path: listelem["fields"]})

        # allkeys = parse_dict(listelem, ret='', level=0)
        # PMGRPCDLOG.info("Cisco: %s: %s" % (proto, allkeys))
        try:
            returned = FinalizeTelemetryData(message_dict)
        except Exception as e:
            PMGRPCDLOG.error("Error finalazing  message: %s", e)


def find_encoding_and_decode(new_msg):
    encoding_type = None
    grpc_message = {}

    # TODO. If options force one type, only try that one.
    # Maybe it is json
    if lib_pmgrpcd.OPTIONS.cenctype == "json":
        PMGRPCDLOG.trace("Try to parse json")
        try:
            grpc_message = json.loads(new_msg.data)
            encoding_type = "ciscojson"
        except Exception as e:
            PMGRPCDLOG.debug(
                "ERROR: Direct json parsing of grpc_message failed with message:\n%s\n",
                e,
            )
        else:
            return encoding_type, grpc_message

    elif lib_pmgrpcd.OPTIONS.cenctype == "gpbkv":
        PMGRPCDLOG.trace("Try to unmarshall KV")
        if encoding_type is None:
            try:
                grpc_message = process_cisco_kv(new_msg)
                encoding_type = "ciscogrpckv"
            except Exception as e:
                PMGRPCDLOG.trace(
                    "ERROR: Parsing of json after unmarshall KV failed with message:\n%s\n",
                    e,
                )
            else:
                return encoding_type, grpc_message

    elif lib_pmgrpcd.OPTIONS.cenctype == "gpbcomp":
        PMGRPCDLOG.trace("Try to unmarshall compact mode")
        PMGRPCDLOG.trace("TODO")

    encoding_type = "unknown"
    return encoding_type, grpc_message
