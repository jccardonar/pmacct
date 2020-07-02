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
import huawei_grpc_dialout_pb2_grpc
from lib_pmgrpcd import PMGRPCDLOG
import ujson as json
import lib_pmgrpcd
from lib_pmgrpcd import ServicerMiddlewareClass, create_grpc_headers
from google.protobuf.json_format import MessageToDict
import time
from datetime import datetime
from export_pmgrpcd import FinalizeTelemetryData, finalize_telemetry_data
import base64
from metric_types.base_types  import GrpcRaw
from metric_types.huawei_metrics import GrpcRawGPBToHuaweiGrpcGPB, HuaweiGrpcGPBToHuaweiCompact, HuaweCompactToHuaweiElements, HuaweDecoderConstructor

# TODO: Maybe move this to its own part, who knows
import huawei_ifm_pb2
import huawei_devm_pb2
import openconfig_interfaces_pb2

#if lib_pmgrpcd.OPTIONS.huawei and (not lib_pmgrpcd.OPTIONS.cenctype == 'gpbkv'):
import huawei_telemetry_pb2

TRACER = lib_pmgrpcd.TRACER.add_labels({"vendor": "Huawei"})




class gRPCDataserviceServicer(huawei_grpc_dialout_pb2_grpc.gRPCDataserviceServicer, metaclass=ServicerMiddlewareClass):
    def __init__(self, huawei_mapper=None):
        if huawei_mapper is None:
            huawei_mapper = {}
        PMGRPCDLOG.info("Huawei: Initializing gRPCDataserviceServicer()")
        self.huawei_mapper_data = huawei_mapper
        if huawei_mapper is not None:
            self.huawei_decoder_constructor = HuaweDecoderConstructor(huawei_mapper)
        else:
            self.huawei_decoder_constructor = None

        super().__init__()

    def dataPublish2(self, message, context):

        # Create grpc metadata
        grpcPeer = create_grpc_headers(context, "Huawei", "huawei_grpc_dialout_pb2_grpc", "GPB Telemetry")
        #grpcPeer = {}
        #grpcPeerStr = context.peer()
        #(
        #    grpcPeer["telemetry_proto"],
        #    grpcPeer["telemetry_node"],
        #    grpcPeer["telemetry_node_port"],
        #) = grpcPeerStr.split(":")
        #grpcPeer["ne_vendor"] = "Huawei"

        PMGRPCDLOG.debug("Huawei MdtDialout Message: %s" % grpcPeer["telemetry_node"])

        #metadata = dict(context.invocation_metadata())
        #grpcPeer["user-agent"] = metadata["user-agent"]
        ## Example of grpcPeerStr -> 'ipv4:10.215.133.23:57775'
        #grpcPeer["grpc_processing"] = "huawei_grpc_dialout_pb2_grpc"
        #grpcPeer["grpc_ulayer"] = "GPB Telemetry"

        jsonTelemetryNode = json.dumps(grpcPeer, indent=2, sort_keys=True)
        PMGRPCDLOG.debug("Huawei RAW Message: %s" % jsonTelemetryNode)

        for new_msg in message:
            PMGRPCDLOG.debug("Huawei new_msg iteration message")
            if lib_pmgrpcd.OPTIONS.ip:
                if grpcPeer["telemetry_node"] != lib_pmgrpcd.OPTIONS.ip:
                    continue
                PMGRPCDLOG.debug(
                    "Huawei: ip filter matched with ip %s"
                    % (lib_pmgrpcd.OPTIONS.ip)
                )
            try:
                huawei_processing(grpcPeer, new_msg)
            except Exception as e:
                PMGRPCDLOG.debug("Error processing Huawei packet, error is %s", e)
                continue
        return
        yield

    def dataPublish(self, message, context):

        # Create grpc metadata
        grpcPeer = create_grpc_headers(context, "Huawei", "huawei_grpc_dialout_pb2_grpc", "GPB Telemetry")

        # the next is done once per connection, so it is fine.
        PMGRPCDLOG.debug("Huawei MdtDialout Message: %s" % grpcPeer["telemetry_node"])
        jsonTelemetryNode = json.dumps(grpcPeer, indent=2, sort_keys=True)

        PMGRPCDLOG.debug("Huawei RAW Message: %s" % jsonTelemetryNode)

        for new_msg in message:

            TRACER.trace_info("msg_received")
            collection_header = grpcPeer.copy()
            received_time = int(round(time.time() * 1000))
            collection_header["received_time"] = received_time
            PMGRPCDLOG.trace("Huawei new_msg iteration message")

            if lib_pmgrpcd.OPTIONS.ip:
                if grpcPeer["telemetry_node"] != lib_pmgrpcd.OPTIONS.ip:
                    TRACER.trace_info("msg_ignored")
                    continue
                PMGRPCDLOG.trace(
                    "Huawei: ip filter matched with ip %s"
                    % (lib_pmgrpcd.OPTIONS.ip)
                )
            try:
                self.huawei_processing(collection_header, new_msg)
            except Exception as e:
                PMGRPCDLOG.trace("Error processing Huawei packet, error is %s", e)
                TRACER.trace_error(e)
                continue
        return
        yield


    def huawei_processing(self, grpcPeer, new_msg):

        # dump the raw data, this has side effects.
        if lib_pmgrpcd.OPTIONS.rawdatadumpfile:
            PMGRPCDLOG.trace("Write rawdatafile: %s" % (lib_pmgrpcd.OPTIONS.rawdatadumpfile))
            with open(lib_pmgrpcd.OPTIONS.rawdatadumpfile, "a") as rawdatafile:
                rawdatafile.write(base64.b64encode(new_msg.data).decode())
                rawdatafile.write("\n")

        data = {"collection_data": grpcPeer, "content": new_msg.data}
        raw_metric = GrpcRaw(data)
        # here, we could provide some decoding options for proto
        huawei_gpb_metric = GrpcRawGPBToHuaweiGrpcGPB().convert(raw_metric)

        # The next part is to get the actual metric type and the 
        # transformation to elements.
        # Many metrics types send multiple measurements in a single packet.
        # The idea is to split themm and treat them individually.
        # If this is not desired, just use the EqualTransformation.
        metric = None
        to_element_transformation = None

        if lib_pmgrpcd.OPTIONS.cenctype == "compact":
            # here, we could provide some decoding options for proto
            huawei_compact_metric = HuaweiGrpcGPBToHuaweiCompact().convert(huawei_gpb_metric)
            metric = huawei_compact_metric
            decoder = self.huawei_decoder_constructor.get_decoder(metric.module)
            to_element_transformation = HuaweCompactToHuaweiElements(decoder)

        if metric is None or to_element_transformation is None:
            raise Exception("Encoding not found, or failing assigning metric")
        # The next is tracing and stats

        for elem in to_element_transformation.transform(metric):
            TRACER.trace_info("element_generated",)
            try:
                returned = finalize_telemetry_data(elem)
            except Exception as e:
                PMGRPCDLOG.trace("Error finalazing  message: %s", e)
                TRACER.trace_error(e)


def huawei_processing(grpcPeer, new_msg):
    PMGRPCDLOG.debug("Huawei: Received GRPC-Data")

    # dump the raw data
    if lib_pmgrpcd.OPTIONS.rawdatadumpfile:
        PMGRPCDLOG.debug("Write rawdatafile: %s" % (lib_pmgrpcd.OPTIONS.rawdatadumpfile))
        with open(lib_pmgrpcd.OPTIONS.rawdatadumpfile, "a") as rawdatafile:
            rawdatafile.write(base64.b64encode(new_msg.data).decode())
            rawdatafile.write("\n")

    try:
        telemetry_msg = huawei_telemetry_pb2.Telemetry()
        telemetry_msg.ParseFromString(new_msg.data)
    except Exception as e:
        PMGRPCDLOG.error(
            "instancing or parsing data failed with huawei_telemetry_pb2.Telemetry"
        )
        PMGRPCDLOG.error("ERROR: %s" % (e))
        raise

    try:
        telemetry_msg_dict = MessageToDict(
            telemetry_msg,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
        )
    except Exception as e:
        PMGRPCDLOG.error(
            "instancing or parsing data failed with huawei_telemetry_pb2.Telemetry"
        )
        raise

    PMGRPCDLOG.debug("Huawei: Received GPB-Data as JSON")
    # TODO: Do we really need this? it can be expensive 
    PMGRPCDLOG.debug(json.dumps(telemetry_msg_dict, indent=2, sort_keys=True))

    message_header_dict = telemetry_msg_dict.copy()

    if "data_gpb" in message_header_dict:
        del message_header_dict["data_gpb"]

    (proto, path) = message_header_dict["sensor_path"].split(":")
    (node_id_str) = message_header_dict["node_id_str"]
    (node_ip) = grpcPeer["telemetry_node"]
    (ne_vendor) = grpcPeer["ne_vendor"]

    # Get the maching L3-Methode
    msg = select_gbp_methode(proto)
    if msg:
        elem = len(telemetry_msg.data_gpb.row)
        epochmillis = int(round(time.time() * 1000))
        PMGRPCDLOG.info(
            "EPOCH=%-10s NIP=%-15s NID=%-20s VEN=%-7s PT=%-22s ET=%-12s ELEM:%s"
            % (epochmillis, node_ip, node_id_str, ne_vendor, proto, "GPB", elem)
        )

        # L2:
        for new_row in telemetry_msg.data_gpb.row:
            # PMGRPCDLOG.info("NEW_ROW: %s" % (new_row))
            # the next converts the object into a dict (x.timestamp, x.content) -> {"timestamp": x, "content": x}
            new_row_header_dict = MessageToDict(
                new_row,
                including_default_value_fields=True,
                preserving_proto_field_name=True,
                use_integers_for_enums=True,
            )

            if "content" in new_row_header_dict:
                del new_row_header_dict["content"]

            # L3:
            msg.ParseFromString(new_row.content)
            content = MessageToDict(
                msg,
                including_default_value_fields=True,
                preserving_proto_field_name=True,
                use_integers_for_enums=True,
            )

            message_dict = {}
            message_dict.update(
                {
                    "collector": {
                        "grpc": {
                            "grpcPeer": grpcPeer["telemetry_node"],
                            "ne_vendor": grpcPeer["ne_vendor"],
                        }
                    }
                }
            )
            message_dict["collector"].update({"data": message_header_dict.copy()})
            message_dict["collector"]["data"].update(new_row_header_dict)
            message_dict.update(content)

            allkeys = parse_dict(content, ret="", level=0)
            PMGRPCDLOG.debug("Huawei: %s: %s" % (proto, allkeys))

            try:
                returned = FinalizeTelemetryData(message_dict)
            except Exception as e:
                PMGRPCDLOG.error("Error finalazing  message: %s", e)



# TODO, probably better to have this in the object

# TODO, probably better to have this in the object


MAP_DICT = None
def get_gpbmapfile():
    global MAP_DICT
    if MAP_DICT is None:
        with open(lib_pmgrpcd.OPTIONS.gpbmapfile, "r") as file:
            MAP_DICT = {}
            for line in file:
                (k, v) = line.split("=")
                # a.e. "huawei-ifm" = 'huawei_ifm_pb2.Ifm()'
                MAP_DICT.update({k.lstrip().rstrip(): v.lstrip().rstrip()})
        PMGRPCDLOG.debug("MAP_DICT: %s", MAP_DICT)
    return MAP_DICT


def select_gbp_methode(proto):
    try:
        map_dict = get_gpbmapfile()
    except:
        PMGRPCDLOG.error(
            "Error getting the map dict"
        )
        raise

    if proto in map_dict:
        PMGRPCDLOG.debug(
            "I FOUND THE GPB (%s) FOR PROTO (%s)" % (proto, map_dict[proto])
        )
        # TODO: I am pretty sure we can do something better than this.
        msg = eval(map_dict[proto])
        return msg
    else:
        PMGRPCDLOG.debug("MISSING GPB Methode for PROTO: %s", proto)
        lib_pmgrpcd.MISSGPBLIB.update({proto: str(datetime.now())})
        return False

def parse_dict(init, ret, level):
    level += 1
    if isinstance(init, dict):
        for key, val in init.items():
            if isinstance(val, dict):
                if level == 1:
                    if key != "grpc":
                        ret = ret + "|" + key
                else:
                    ret = ret + "->" + key
                ret = parse_dict(val, ret, level)
            if isinstance(val, list):
                for liit in val:
                    ind = val.index(liit)
                    if isinstance(liit, dict):
                        if level == 1:
                            if liit != "grpc":
                                ret = ret + "|" + key + "->[" + str(ind) + "]"
                        else:
                            ret = ret + "->" + key + "->[" + str(ind) + "]"
                        ret = parse_dict(liit, ret, level)
    return ret
