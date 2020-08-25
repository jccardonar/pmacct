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
import os
import time
from lib_pmgrpcd import PMGRPCDLOG, trace_warning
import lib_pmgrpcd
import sys
# TODO: Got a sefault with ujson and with rapidjson. Evaluate why
import ujson as json
#import json as json_l
from abc import ABC, abstractmethod
from debug import get_lock
from exceptions import PmgrpcdException
from transformations.transformations import metric_to_json_dict
import logging


jsonmap = {}
avscmap = {}


example_dict = {}

EXPORTERS = {}
TRANSFORMATION = None

class Exporter(ABC):

    def __init__(self, logger=None):
        if logger is None:
            logger = logging.getLogger()
        self.logger = logger

    @abstractmethod
    def process_metric(self, metric):
        pass

class NoNewMetrics(PmgrpcdException):
    pass

def export_metrics(datajsonstring):
    #breakpoint() if get_lock() else None
   
    for exporter in EXPORTERS:
        try:
            EXPORTERS[exporter].process_metric(datajsonstring)
        except Exception as e:
            PMGRPCDLOG.debug("Error processing packet on exporter %s. Error was %s", exporter, e)
            raise
        #breakpoint() if get_lock() else None


def examples(dictTelemetryData_mod, jsonTelemetryData):
    global example_dict
    if dictTelemetryData_mod["collector"]["grpc"]["grpcPeer"]:
        grpcPeer = dictTelemetryData_mod["collector"]["grpc"]["grpcPeer"]
        if dictTelemetryData_mod["collector"]["grpc"]["ne_vendor"]:
            ne_vendor = dictTelemetryData_mod["collector"]["grpc"]["ne_vendor"]
            if dictTelemetryData_mod["collector"]["data"]["encoding_path"]:
                encoding_path = dictTelemetryData_mod["collector"]["data"][
                    "encoding_path"
                ]

                PMGRPCDLOG.debug(
                    "IN EXAMPLES: grpcPeer=%s ne_vendor=%s encoding_path=%s"
                    % (grpcPeer, ne_vendor, encoding_path)
                )

    try:
        if not os.path.exists(lib_pmgrpcd.OPTIONS.examplepath):
            os.makedirs(lib_pmgrpcd.OPTIONS.examplepath)
    except OSError:
        pass
    if grpcPeer not in example_dict:
        example_dict.update({grpcPeer: []})

    if encoding_path not in example_dict[grpcPeer]:
        example_dict[grpcPeer].append(encoding_path)
        encoding_path_mod = encoding_path.replace(":", "_").replace("/", "-")

        exafilename = grpcPeer + "_" + ne_vendor + "_" + encoding_path_mod + ".json"
        exapathfile = os.path.join(lib_pmgrpcd.OPTIONS.examplepath, exafilename)

        with open(exapathfile, "w") as exapathfile:
            # exapathfile.write("PROTOPATH[" + telemetry_node + "]: " + protopath + "\n")
            exapathfile.write(jsonTelemetryData)
            exapathfile.write("\n")


def FinalizeTelemetryData(dictTelemetryData):

    # Adding epoch in millisecond to identify this singel metric on the way to the storage
    epochmillis = int(round(time.time() * 1000))
    dictTelemetryData["collector"]["data"].update({"collection_timestamp": epochmillis})

    dictTelemetryData_mod = dictTelemetryData.copy()

    # Going over the mitigation library, if needed.
    # TODO: Simplify the next part
    dictTelemetryData_beforeencoding = None
    if lib_pmgrpcd.OPTIONS.mitigation:
        from mitigation import mod_all_json_data
        try:
            dictTelemetryData_mod = mod_all_json_data(dictTelemetryData_mod)
            dictTelemetryData_beforeencoding = dictTelemetryData_mod
            jsonTelemetryData = json.dumps(
                dictTelemetryData_mod, indent=2, sort_keys=True
            )
        except Exception as e:
            PMGRPCDLOG.info("ERROR: mod_all_json_data raised a error:\n%s")
            PMGRPCDLOG.info("ERROR: %s" % (e))
            dictTelemetryData_mod = dictTelemetryData
            dictTelemetryData_beforeencoding = dictTelemetryData
            jsonTelemetryData = json.dumps(dictTelemetryData, indent=2, sort_keys=True)
    else:
        dictTelemetryData_mod = dictTelemetryData
        dictTelemetryData_beforeencoding = dictTelemetryData
        # TODO: issue with seg fault seesm to be here, and the reason seems to be that indent
        #print("here", dictTelemetryData_beforeencoding["collector"]["data"]["path"])
        #if dictTelemetryData_beforeencoding["collector"]["data"]["path"] == "sys/ipqos":
        #    json.dump(dictTelemetryData, open("seg_fault.json", "w"), sort_keys=True)

        jsonTelemetryData = json.dumps(dictTelemetryData, sort_keys=True)
        #jsonTelemetryData = json_l.dumps(dictTelemetryData, indent=2, sort_keys=True)
        #jsonTelemetryData = json.dumps(dictTelemetryData, sort_keys=True)

    PMGRPCDLOG.debug("After mitigation: %s" % (jsonTelemetryData))

    # Check if we need to transform. This will change later
    #breakpoint() if get_lock() else None
    path = dictTelemetryData_beforeencoding["collector"]["data"]["path"]
    actual_data  = dictTelemetryData_beforeencoding.get(path, {})
    #if path == "sys/intf":
    #    return
    #breakpoint() if get_lock() else None



    #if TRANSFORMATION and dictTelemetryData_beforeencoding and "dataGpbkv" in dictTelemetryData_beforeencoding.get("collector", {}).get("data", {}):
    #    data = dictTelemetryData_beforeencoding["collector"]["data"].copy()
    #    data["dataGpbkv"] = [{"fields": actual_data}]
    #    # we just transform for kv
    #    metric = CiscoKVFlatten.build_from_dcit(data)
    #    internals = list(metric.get_internal())

    #    #breakpoint() if get_lock() else None
    #    #if not ":" in path:
    #    #    # we guess it is NX.
    #    #    nx_metrics = list(NXEncoder.build_from_internal(x) for x in internals)
    #    #    internals = []
    #    #    for nx_metric in nx_metrics:
    #    #        for internal in nx_metric.get_internal():
    #    #            internals.append(internal)

    #    #breakpoint() if get_lock() else None
    #    for internal in internals:
    #        for new_metric in TRANSFORMATION.transform(internal):
    #            print(new_metric.keys)
    #            data = new_metric.data
    #            data["dataGpbkv"] = new_metric.content
    #            export_metrics(json.dumps({"collector": {"data":data}}))
    #    #breakpoint() if get_lock() else None
    #    return jsonTelemetryData
    #breakpoint() if get_lock() else None


    if lib_pmgrpcd.OPTIONS.examplepath and lib_pmgrpcd.OPTIONS.example:
        examples(dictTelemetryData_mod, jsonTelemetryData)

    if lib_pmgrpcd.OPTIONS.jsondatadumpfile:
        PMGRPCDLOG.debug("Write jsondatadumpfile: %s" % (lib_pmgrpcd.OPTIONS.jsondatadumpfile))
        with open(lib_pmgrpcd.OPTIONS.jsondatadumpfile, "a") as jsondatadumpfile:
            jsondatadumpfile.write(jsonTelemetryData)
            jsondatadumpfile.write("\n")


    # Filter only config.
    export = True
    if lib_pmgrpcd.OPTIONS.onlyopenconfig:
        PMGRPCDLOG.debug(
            "only openconfig filter matched because of options.onlyopenconfig: %s"
            % lib_pmgrpcd.OPTIONS.onlyopenconfig
        )
        export = False
        if "encoding_path" in dictTelemetryData_mod["collector"]["data"]:
            if (
                "openconfig"
                in dictTelemetryData_mod["collector"]["data"]["encoding_path"]
            ):
                export = True


    if export:
        export_metrics(jsonTelemetryData)

    return jsonTelemetryData

def finalize_telemetry_data(metric):
    # Filter only config.
    export = True
    if lib_pmgrpcd.OPTIONS.onlyopenconfig:
        PMGRPCDLOG.debug(
            "only openconfig filter matched because of options.onlyopenconfig: %s"
            % lib_pmgrpcd.OPTIONS.onlyopenconfig
        )
        export = False
        if "openconfig" in metric.path:
            export = True
    if not export:
        return

    metrics = [metric]
    path = metric.path

    warnings = []


    # find mitigation
    if TRANSFORMATION:
        try:
            new_metrics = list(TRANSFORMATION.transform_list(metrics, warnings))
        except:
            raise
        if not new_metrics:
            raise NoNewMetrics("No new metrics fromm transformations", {"transformation": "TRANSFORMATIONS", "path": path})
        metrics = new_metrics
        if warnings:
            trace_warning(warnings)

    if lib_pmgrpcd.MITIGATION:
        try:
            new_metrics = list(lib_pmgrpcd.MITIGATION.transform_list(metrics, warnings))
        except:
            raise
        if not new_metrics:
            raise NoNewMetrics("No new metrics fromm transformations", {"transformation": "MITIGATION", "path": path})
        metrics = new_metrics
        if warnings:
            trace_warning(warnings)


    for n_metric in new_metrics:
        # this can change if content is a dict or list
        data = metric_to_json_dict(n_metric)
        json_metric = json.dumps(data)
        export_metrics2(json_metric)


def export_metrics2(json_metric: str):
    for exporter in EXPORTERS:
        try:
            EXPORTERS[exporter].process_metric(json_metric)
        except Exception as e:
            PMGRPCDLOG.debug("Error processing packet on exporter %s. Error was %s", exporter, e)
            raise
        #breakpoint() if get_lock() else None


