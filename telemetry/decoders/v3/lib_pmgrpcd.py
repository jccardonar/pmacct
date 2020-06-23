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
import logging
from pathlib import Path
from types import MethodType
from typing import Tuple

SCRIPTVERSION = "1.4"


PMGRPCDLOG = logging.getLogger("PMGRPCDLOG")
OPTIONS = None
MISSGPBLIB = {}


def parser_grpc_peer(peer: str) -> Tuple[str, str, str]:
    '''
    Splits the GRPC peer name into parts.
    >>> parser_grpc_peer("ipv6:[::1]:53236")
    ('ipv6', '[::1]', '53236')
    >>> parser_grpc_peer("ipv4:0.0.0.0:0")
    ('ipv4', '0.0.0.0', '0')
    >>> parser_grpc_peer('any randon stuff')
    ('', 'any randon stuff', '')
    >>> parser_grpc_peer('nosense:localhost')
    ('nosense', 'localhost', '')
    >>> parser_grpc_peer('no sense : with many ::: dots')
    ('no sense ', ' with many ::', ' dots')
    >>> parser_grpc_peer('no sense : with many ::: dots:')
    ('no sense ', ' with many ::: dots', '')
    '''
    splitter = ":"
    # we try to get the port
    rest, separator, port = peer.rpartition(splitter)
    if separator != splitter:
        # if there is no :, we assume everything is the node.
        return "", peer, ""
    if splitter not in rest:
        return rest, port, ""
    # there must be a : here, we assume the first part is the protocol name (ipv4, ipv6)
    protocol, separator, node = rest.partition(splitter)
    return protocol, node, port


TRACE_LEVEL = 5


def add_trace_function_to_logger(logger):
    """
    We "patch" a logger to have a tracer function with level 1 to emit msgs per packet.
    This is synthatic sugar for
    log(TRACE_LEVEL, msg, *args, **kargs)
    from https://block.arch.ethz.ch/blog/2016/07/adding-methods-to-python-classes/
    """
    logging.addLevelName(5, "TRACE")

    def trace(self, msg, *args, **kargs):
        return self.log(TRACE_LEVEL, msg, *args, **kargs)

    logger.trace = MethodType(trace, logger)


class CollectorState:
    def __init__(self, config_options):
        self.config_options = config_options

    @property
    def OPTIONS(self):
        return self._config_options

    @property
    def config_options(self):
        return self._config_options

    @config_options.setter
    def config_options(self, config_options):
        # validation can fall here
        self._config_options = config_options


STATE = None


def get_state():
    if STATE is None:
        raise Exception("State has not been inizialized")
    return STATE


def init_pmgrpcdlog(config_options):
    global PMGRPCDLOG, OPTIONS
    global STATE

    STATE = CollectorState(config_options)

    configure_logging()


def configure_logging():

    config_options = get_state().config_options

    if config_options.logging_config_file:
        logging.config.fileConfig(config_options.logging_config_file)
    else:
        grformatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        add_trace_function_to_logger(PMGRPCDLOG)

        if config_options.debug:
            PMGRPCDLOG.setLevel(logging.DEBUG)
        else:
            PMGRPCDLOG.setLevel(logging.INFO)

        # create file handler which logs even debug messages
        grfh = logging.FileHandler(config_options.PMGRPCDLOGfile)
        # if config_options.debug:
        #    grfh.setLevel(logging.DEBUG)
        # else:
        #    grfh.setLevel(logging.INFO)

        grfh.setFormatter(grformatter)
        PMGRPCDLOG.addHandler(grfh)

        if config_options.console:
            # create console handler with a higher log level
            grch = logging.StreamHandler()
            # if config_options.debug:
            #    grch.setLevel(logging.DEBUG)
            # else:
            #    grch.setLevel(logging.INFO)

            grch.setFormatter(grformatter)
            PMGRPCDLOG.addHandler(grch)

        init_serializelog()


def init_serializelog():
    global SERIALIZELOG
    config_options = get_state().config_options
    SERIALIZELOG = logging.getLogger("SERIALIZELOG")
    SERIALIZELOG.setLevel(logging.DEBUG)
    seformatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # create file handler which logs even debug messages
    sefh = logging.FileHandler(config_options.serializelogfile)
    if config_options.debug:
        sefh.setLevel(logging.DEBUG)
    else:
        sefh.setLevel(logging.INFO)

    sefh.setFormatter(seformatter)
    SERIALIZELOG.addHandler(sefh)

    if config_options.console:
        # create console handler with a higher log level
        sech = logging.StreamHandler()
        if config_options.debug:
            sech.setLevel(logging.DEBUG)
        else:
            sech.setLevel(logging.INFO)

        sech.setFormatter(seformatter)
        SERIALIZELOG.addHandler(sech)


def signalhandler(signum, frame):
    global MISSGPBLIB
    # pkill -USR1 -e -f "python.*pmgrpc"
    if signum == 10:
        PMGRPCDLOG.info("Signal handler called with USR1 signal: %s" % (signum))
        PMGRPCDLOG.info("These are the missing gpb libs: %s" % (MISSGPBLIB))
    if signum == 12:
        PMGRPCDLOG.info("Signal handler called with USR2 signal: %s" % (signum))
        PMGRPCDLOG.info("TODO: %s" % ("todo"))

