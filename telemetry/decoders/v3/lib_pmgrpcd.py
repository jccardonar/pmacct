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
from statsd_metrics_exporter import StatsDMetricExporter
from functools import wraps
import inspect

SCRIPTVERSION = "1.4"


PMGRPCDLOG = logging.getLogger("PMGRPCDLOG")
OPTIONS = None
MISSGPBLIB = {}
METRIC_EXPORTER = None


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


def create_grpc_headers(context, vendor, processing, ulayer):
    grpcPeer = {}
    grpcPeerStr = context.peer()
    (
        grpcPeer["telemetry_proto"],
        grpcPeer["telemetry_node"],
        grpcPeer["telemetry_node_port"],
    ) = parser_grpc_peer(grpcPeerStr)
    grpcPeer["ne_vendor"] = vendor
    metadata = dict(context.invocation_metadata())
    grpcPeer["user-agent"] = metadata["user-agent"]
    # Example of grpcPeerStr -> 'ipv4:10.215.133.23:57775'
    grpcPeer["grpc_processing"] = processing
    grpcPeer["grpc_ulayer"] = ulayer
    return grpcPeer

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

def setup_metric_exporter():
    global METRIC_EXPORTER
    if OPTIONS.metrics_server_enable:
        METRIC_EXPORTER = StatsDMetricExporter(OPTIONS.metrics_ip, OPTIONS.metrics_port, prefix=OPTIONS.metrics_name_prefix)

def get_logger_level():
    config_options = OPTIONS
    if config_options.trace:
        return TRACE_LEVEL
    if config_options.debug:
        return logging.DEBUG
    return logging.INFO


def configure_logging():

    setup_metric_exporter()

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

        if config_options.trace:
            PMGRPCDLOG.setLevel(TRACE_LEVEL)

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


class Tracer:

    def __init__(self, fixed_labels=None):
        if fixed_labels is None:
            fixed_labels = {}
        self.fixed_labels = dict(fixed_labels)

    def add_labels(self, new_labels):
        labels = dict(new_labels)
        labels.update(self.fixed_labels)
        return self.__class__(labels)

    def extend_labels(self, labels):
        if not self.fixed_labels:
            return labels
        new_labels = dict(labels)
        new_labels.update(self.fixed_labels)
        return new_labels
        

    def process_trace_data(self, msg, labels=None, log_type="info", log=False) -> None:
        '''
        Here we lose the ability of appending values to a msg, but since this is thought 
        mainly for metrics, it is fine.
        '''
        config_options = get_state().config_options
        if not config_options.metrics_server_enable:
            return

        if labels is None:
            labels = {}

        if isinstance(msg, Exception):
            metric_name = getattr(msg, "metric_name", None)
            if metric_name is None:
                metric_name = msg.__class__.__name__
            excep_labels = getattr(msg, "fields", {})
            excep_labels.update(labels)
            labels = excep_labels
        else:
            metric_name = str(msg)

        labels["type"] = log_type
        labels = self.extend_labels(labels)

        # Send metrics if needed
        if METRIC_EXPORTER:
            try:
                METRIC_EXPORTER.incr(metric_name, labels)
            except Exception as e:
                # this is kind of counterproductive, but this cannot fail silently
                PMGRPCDLOG.error(f"Failing sending metric with error: {e}. Disable metric exporter if this generates too many logs.")

        if log and config_options.trace and config_options.metrics_send_to_log:
            # Log using the trace, we lose the replacement part here
            PMGRPCDLOG.trace(str(msg))


    def trace_warning(self, msg, labels=None, log=False):
        self.process_trace_data(msg, labels, "warning", log)

    def trace_error(self, msg, labels=None, log=False):
        self.process_trace_data(msg, labels, "error", log)

    def trace_info(self, msg, labels=None, log=False):
        self.process_trace_data(msg, labels, "info", log)


TRACER = Tracer()

MITIGATION = None

def trace_warning(*args, **kargs):
    TRACER.trace_warnings(*args, **kargs)

def trace_error(*args, **kargs):
    TRACER.trace_warnings(*args, **kargs)


def log_wrapper(method):

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
