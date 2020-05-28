"""
The overall strategy for configuration is to have the next priority order:
    command line > env variables > configuration file > defaults.

Given this priority, we use  a "flat" configuration object which is, basically, the result of the argument parser from the ConfigArgParse library.

The library allows for sections in the config file, but that's just for referencing. The names of the attributes are global, and thus cannot be repeated.

We might have multiple sections in the future, but then we will have to change the configuration and state strategy.
"""
from pathlib import Path

# define defaults here


def configure_parser(parser):
    add_general(parser)
    add_logging(parser)
    add_grpc_server(parser)
    add_development(parser)

    add_input(parser)
    add_transformation(parser)
    add_output(parser)

    add_kafka(parser)


def add_general(parser):

    parser.add(
        "-c",
        dest="configuration",
        is_config_file=True,
        help="Path to configuration file",
    )

    parser.add(
        "-v", action="store_true", dest="version", help="print version of this script"
    )


def add_development(parser):
    """
    Development options. Do we need all of this?
    """
    parser.add(
        "-e",
        "--example",
        action="store_true",
        dest="example",
        help="Enable writing Example Json-Data-Files",
    )

    parser.add(
        "-E",
        "--examplepath",
        dest="examplepath",
        help="dump a json example of each proto/path to this examplepath",
    )
    parser.add(
        "-A",
        "--avscid",
        dest="avscid",
        help="this is to serialize manually with avscid and jsondatafile (for development)",
    )

    parser.add(
        "-J",
        "--jsondatafile",
        dest="jsondatafile",
        help="this is to serialize manually with avscid and jsondatafile (for development)",
    )

    parser.add(
        "-R",
        "--rawdatafile",
        dest="rawdatafile",
        help="this is to process manually (via mitigation) process a rawdatafile with a single rawrecord (for development)",
    )


def add_grpc_server(parser):
    """
    Elements used to configure grpc server
    """
    parser.add(
        "-I",
        "--ipport",
        action="store",
        dest="ipport",
        help="change the ipport the daemon is listen on",
    )

    # Workers for the server pool
    parser.add(
        "-w",
        "--workers",
        action="store",
        type=int,
        dest="workers",
        help="change the nr of paralell working processes",
    )

    # certificate paths
    parser.add(
        "--grpc_server_private_key",
        help="File with the private key for the grpc server",
    )

    parser.add(
        "--grpc_server_certificate_chain",
        help="File with the certificate chain for the grpc server",
    )


def add_input(parser):
    """
    Input options. Not all options are needed for all inputs.
    """

    parser.add(
        "-C",
        "--cisco",
        action="store_true",
        dest="cisco",
        help="enable the grpc messages comming from Cisco",
    )

    parser.add(
        "--nx", action="store_true", dest="nx_enable", help="Enable NX processing"
    )

    parser.add(
        "-H",
        "--huawei",
        action="store_true",
        dest="huawei",
        help="enable the grpc messages comming from Huawei",
    )

    parser.add(
        "-t",
        "--cenctype",
        type=str,
        dest="cenctype",
        help="cenctype is the type of encoding for cisco. This is because some protofiles are incompatible. With cenctype=gpbkv only cisco is enabled. The encoding type can be json, gpbcomp, gpbkv",
    )

    parser.add("-i", "--ip", dest="ip", help="only accept pakets of this single ip")

    parser.add(
        "-o",
        "--onlyopenconfig",
        action="store_true",
        dest="onlyopenconfig",
        help="only accept packets of openconfig",
    )

    parser.add(
        "-r",
        "--rawdatadumpfile",
        dest="rawdatadumpfile",
        help="writing the raw data from the routers to the rowdatafile path/name",
    )

    parser.add(
        "--file_importer_file",
        dest="file_importer_file",
        help="Name of the file to import. If set, we will ignore the rest of the importers.",
    )


def add_output(parser):
    """
    General output options. Specific options might be on other functions.
    """
    parser.add(
        "-k",
        "--kafkaavro",
        action="store_true",
        dest="kafkaavro",
        help="enable forwarding to Kafka kafkaavro (with schema-registry)",
    )

    parser.add(
        "-s",
        "--kafkasimple",
        dest="kafkasimple",
        action="store_true",
        help="Boolean if kafkasimple should be enabled.",
    )

    parser.add(
        "--file_exporter_file",
        dest="file_exporter_file",
        help="Name of file for file exporter.",
    )

    parser.add(
        "-j",
        "--jsondatadumpfile",
        dest="jsondatadumpfile",
        help="writing the output to the jsondatadumpfile path/name",
    )


def add_logging(parser):

    parser.add(
        "--logging_config_file",
        dest="logging_config_file",
        help="File with logging config. If this is given, the rest of parameters are ignored.",
    )

    parser.add(
        "-l",
        "--PMGRPCDLOGfile",
        dest="PMGRPCDLOGfile",
        required=True,
        help="PMGRPCDLOGfile the logfile on the collector face with path/name",
    )

    parser.add(
        "-a",
        "--serializelogfile",
        dest="serializelogfile",
        help="serializelogfile with path/name for kafka avro and zmq messages",
    )

    parser.add(
        "-d",
        "--debug",
        action="store_true",
        dest="debug",
        help="enable debug messages on the logfile",
    )

    parser.add(
        "-N",
        "--console",
        action="store_true",
        dest="console",
        help="this is to display all log-messages also on console (for development)",
    )

    # The next are for packet events.
    # packet events will overflow any logging, therefore, we prefer to use metrics
    # packet events can be of error or debug. 
    parser.add("--log_packet_events", action="store_true", help="Logs packet events as debug")
    parser.add("--send_metric_packet_events", action="store_true", help="Sends packet evetns to stats (requires stats_ipport to be configured)")
    parser.add("--stats_ipport", help="Ip and port of stats server")

def add_kafka(parser):
    # Topic and servers
    parser.add(
        "-T",
        "--topic",
        env_var="PM_TOPIC",
        dest="topic",
        help="The json data are serialized to this topic in kafka",
    )

    parser.add(
        "-B",
        "--bsservers",
        env_var="BSSERVERS",
        dest="bsservers",
        help="bootstrap servers url with port to reach kafka",
    )

    # Certificates
    parser.add(
        "-S",
        "--secproto",
        default="ssl",
        dest="secproto",
        help="security protocol (Normaly ssl)",
    )

    parser.add(
        "-O",
        "--sslcertloc",
        env_var="SSLCERTLOC",
        dest="sslcertloc",
        help="path/file to ssl certification location",
    )

    parser.add(
        "-K",
        "--sslkeyloc",
        env_var="SSLKEYLOC",
        dest="sslkeyloc",
        help="path/file to ssl key location",
    )

    # Kafka registry values (only if needed)
    parser.add(
        "-U",
        "--urlscreg",
        env_var="URLSCREG",
        dest="urlscreg",
        help="the url to the schema-registry",
    )

    parser.add(
        "-L",
        "--calocation",
        env_var="CALOCATION",
        dest="calocation",
        help="the ca_location used to connect to schema-registry",
    )

    # GBP map file and ascmap file
    parser.add(
        "-G",
        "--gpbmapfile",
        env_var="GPBMAPFILE",
        dest="gpbmapfile",
        help="change path/name of gpbmapfile",
    )

    parser.add(
        "-M",
        "--avscmapfile",
        env_var="AVSCMALFILE",
        dest="avscmapfile",
        help="path/name to the avscmapfile",
    )

    parser.add(
        "--file_topic_per_encoding_path",
        dest="file_topic_per_encoding_path",
        help="Json file indentifing the topic per encoding path.",
    )


def add_transformation(parser):

    parser.add(
        "-m",
        "--mitigation",
        action="store_true",
        dest="mitigation",
        help="enable plugin mitigation mod_result_dict from python module mitigation.py",
    )

    parser.add(
        "--file_transformations",
        dest="file_transformations",
        help="Json file detailing the transformations to execute.",
    )
