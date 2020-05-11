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
from optparse import OptionParser
import configparser
import os
from datetime import datetime
import sys
from typing import Dict
from distutils.util import strtobool 

# TODO we'll move the next into more appropiate places
from lib_pmgrpcd import (
    SCRIPTVERSION,
    init_pmgrpcdlog,
    PMGRPCDLOG,
    FileNotFound,
    init_serializelog,
    signalhandler,
)
import lib_pmgrpcd
import signal

from concurrent import futures
# gRPC and Protobuf imports
import grpc
import cisco_grpc_dialout_pb2_grpc
import huawei_grpc_dialout_pb2_grpc
import time
from config import configure
from file_modules.file_input import FileInput
from pathlib import Path
import os
# from gnmi_pmgrpcd import GNMIClient
from kafka_modules.kafka_avro_exporter import manually_serialize
from option_parser_construction import configure_parser

import  configargparse

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
FOLDER_EXTRA_FILES = Path("config_files")
ABSOLUTE_FILE = Path(__file__).resolve()
CONFIGDIR = ABSOLUTE_FILE.parent / FOLDER_EXTRA_FILES
DEFAULT_CONFIGFILE = CONFIGDIR / "telemetry.conf"
CONFIGFILE = str(DEFAULT_CONFIGFILE)
            

def main():
    global CONFIGFILE
    version_str = "%prog " + SCRIPTVERSION

    # Parse arguments. Default must be a named argument!
    parser = configargparse.ArgParser(default_config_files=[str(DEFAULT_CONFIGFILE)])
    # the next one is not really used, but important to avoid errors.
    #gnmi options
    #parser.add_option(
    #    "-g",
    #    "--gnmi_enable",
    #    default=config.getboolean("PMGRPCD", "gnmi_enable", fallback=False),
    #    help="Boolean defining whether gnmi is enable (this disables the rest of collectrors)",
    #)
    #parser.add_option(
    #    "--gnmi_target",
    #    env_name = "GNMI_SERVER",
    #    default=config.get("PMGRPCD", "gnmi_target", fallback=None),
    #    help="The url of the gnmi target",
    #)
    configure_parser(parser)

    lib_pmgrpcd.OPTIONS = parser.parse_args()

    init_pmgrpcdlog()
    init_serializelog()

    if lib_pmgrpcd.OPTIONS.version:
        print("Version ", version_str)
        raise SystemExit


    PMGRPCDLOG.info("Using %s as config file",  CONFIGFILE)
    PMGRPCDLOG.info("startoptions of this script: %s", str(lib_pmgrpcd.OPTIONS))

    # Test-Statements Logging
    # -----------------------
    # PMGRPCDLOG.debug('debug message')
    # PMGRPCDLOG.info('info message')
    # PMGRPCDLOG.warning('warn message')
    # PMGRPCDLOG.error('error message')
    # PMGRPCDLOG.critical('critical message')

    # serializelog.debug('debug message')
    # serializelog.info('info message')
    # serializelog.warning('warn message')
    # serializelog.error('error message')
    # serializelog.critical('critical message')

    configure()


    PMGRPCDLOG.info("enable listening to SIGNAL USR1 with Sinalhandler")
    signal.signal(signal.SIGUSR1, signalhandler)
    PMGRPCDLOG.info("enable listening to SIGNAL USR2 with Sinalhandler")
    signal.signal(signal.SIGUSR2, signalhandler)

    # I am going to comment the manually export of data from now, this could go into other script.
    if lib_pmgrpcd.OPTIONS.avscid and lib_pmgrpcd.OPTIONS.jsondatafile:
        manually_serialize()
    elif lib_pmgrpcd.OPTIONS.file_importer_file:
        file_importer = FileInput(lib_pmgrpcd.OPTIONS.file_importer_file)
        PMGRPCDLOG.info("Starting file import")
        file_importer.generate()
        PMGRPCDLOG.info("No more data, sleeping 3 secs")
        time.sleep(3)
        PMGRPCDLOG.info("Finalizing file import")
    elif lib_pmgrpcd.OPTIONS.avscid or lib_pmgrpcd.OPTIONS.jsondatafile:
        PMGRPCDLOG.info(
            "manually serialize need both lib_pmgrpcd.OPTIONS avscid and jsondatafile"
        )
        parser.print_help()
    #elif lib_pmgrpcd.OPTIONS.gnmi_enable:
    #    if lib_pmgrpcd.OPTIONS.gnmi_target is None:
    #        error = "gnmi target not configured, but gnmi enabled"
    #        PMGRPCDLOG.error(error)
    #        raise Exception(error)
    #
    #    PMGRPCDLOG.info("Starting contact with gnmi server %s. Other functions will be ignored", lib_pmgrpcd.OPTIONS.gnmi_target)
    #    channel = grpc.insecure_channel(lib_pmgrpcd.OPTIONS.gnmi_target)
    #    gnmi_client = GNMIClient(channel)
    #    breakpoint()

    else:
        # make sure some important files exist
        if not os.path.isfile(lib_pmgrpcd.OPTIONS.gpbmapfile):
            raise FileNotFound("No gpbmapfile file found in {}".format(lib_pmgrpcd.OPTIONS.gpbmapfile))

        # TODO: Do we really need this always?
        if not os.path.isfile(lib_pmgrpcd.OPTIONS.avscmapfile):
            raise FileNotFound("No avscmapfile file found in {}".format(lib_pmgrpcd.OPTIONS.avscmapfile))
        PMGRPCDLOG.info("pmgrpsd.py is started at %s", str(datetime.now()))
        serve()


def serve():

    gRPCserver = grpc.server(
        futures.ThreadPoolExecutor(max_workers=lib_pmgrpcd.OPTIONS.workers)
    )

    if lib_pmgrpcd.OPTIONS.huawei:
        if lib_pmgrpcd.OPTIONS.cenctype == 'gpbkv':
            PMGRPCDLOG.info("Huawei is disabled because cenctype=gpbkv")
        else:
            PMGRPCDLOG.info("Huawei is enabled")
            # Ugly, but we have to load just here because if not there is an exception due to a conflict between the cisco and huawei protos.
            from huawei_pmgrpcd import gRPCDataserviceServicer
            huawei_grpc_dialout_pb2_grpc.add_gRPCDataserviceServicer_to_server(
                gRPCDataserviceServicer(), gRPCserver
            )
    else:
        PMGRPCDLOG.info("Huawei is disabled")

    if lib_pmgrpcd.OPTIONS.cisco:
        PMGRPCDLOG.info("Cisco is enabled")
        # Ugly, but we have to load just here because if not there is an exception due to a conflict between the cisco and huawei protos.
        from cisco_pmgrpcd import gRPCMdtDialoutServicer
        cisco_grpc_dialout_pb2_grpc.add_gRPCMdtDialoutServicer_to_server(
            gRPCMdtDialoutServicer(), gRPCserver
        )
    else:
        PMGRPCDLOG.info("Cisco is disabled")

    gRPCserver.add_insecure_port(lib_pmgrpcd.OPTIONS.ipport)
    gRPCserver.start()

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        gRPCserver.stop(0)
        PMGRPCDLOG.info("Stopping server")
        time.sleep(1)


if __name__ == "__main__":
    main()
