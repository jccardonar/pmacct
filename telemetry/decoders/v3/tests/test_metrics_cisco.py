"""
Tests cisco metrics construction, transformation and operations.
"""
import pytest
from .utils_test import (
    data_folder,
    load_dump_file,
    AbstractTestMetric,
    pytest_generate_tests,
)
from metric_types.base_types import GrpcRaw
from metric_types.cisco.cisco_metrics import (
    CiscoGrpcGPB,
    EncodingNotFound,
    CiscoEncodings,
    CiscoGrpcKV,
    CiscoGrpcJson,
    CiscoElement,
    CiscoGrpcGPB,
    EncodingNotFound,
    CiscoEncodings,
    CiscoGrpcKV,
    CiscoElement,
    GrpcRawToCiscoGrpcGPB,
    GrpcRawJsonToCiscoGrpcGPB,
    CiscoGrpcGPBToCiscoGrpcJson,
    CiscoGrpcJsonToCiscoElement,
    GrpcRawGPBToCiscoGrpcGPB,
    CiscoGrpcGPBToCiscoGrpcKV,
)
from metric_types.cisco.nx import (
    NxGrpcGPB,
    NXGrpcKV,
    NXElement,
    GrpcRawToNxGrpcGPB,
    NxGrpcGPBToNXGrpcKV,
    CiscoElementToNXElement,
)
from .utils_test import check_metric_properties, check_basic_properties
from metric_types.cisco.cisco_gpbvkv import PivotingCiscoGPBKVDict
from metric_types.cisco.nx_api import PivotingNXApiDict

DATA_FOLDER = data_folder()
CISCO_DUMP_FOLDER = DATA_FOLDER / "cisco_dumps"
CISCO_FILES = [x for x in CISCO_DUMP_FOLDER.iterdir() if x.is_file()]

# Data for a GRPC cisco packet. This covers a json or GPB decoded, all of them "Should" follow the telemetry.proto style.
JSON_DATA = [
    {
        "node_id_str": "r14.labxtx01.us.bb",
        "subscription_id_str": "gtat-state-pmacct",
        "encoding_path": "Cisco-IOS-XR-nto-misc-oper:memory-summary/nodes/node/summary",
        "collection_id": "11",
        "collection_start_time": "1590790951700",
        "msg_timestamp": "1590790951707",
        "collection_end_time": "1590790951716",
        "data_json": [
            {
                "timestamp": "1590790951706",
                "keys": [{"node-name": "0/RP0/CPU0"}],
                "content": {
                    "page-size": 4096,
                    "ram-memory": "28757590016",
                    "free-physical-memory": "21408897024",
                    "system-ram-memory": "28757590016",
                    "free-application-memory": "21408897024",
                    "image-memory": "4194304",
                    "boot-ram-size": "0",
                    "reserved-memory": "0",
                    "io-memory": "0",
                    "flash-system": "0",
                },
            }
        ],
    }
]

KV_DATA = [
    {
        "node_id_str": "r14.labxtx01.us.bb",
        "subscription_id_str": "gtat-state-pmacct",
        "encoding_path": "Cisco-IOS-XR-l2-eth-infra-oper:mac-accounting/interfaces/interface",
        "collection_id": "80116",
        "collection_start_time": "1561673093023",
        "msg_timestamp": "1561673093023",
        "collection_end_time": "1561673093037",
    }
]

NX_API_DATA = [
    {
        "node_id_str": "n9k",
        "subscription_id_str": "1",
        "encoding_path": "sys/intf",
        "collection_id": "67",
        "msg_timestamp": "1586033800397",
        "data_gpbkvd": [
            {
                "fields": [
                    {
                        "name": "keys",
                        "fields": [{"name": "sys/intf", "string_value": "sys/intf"}],
                    },
                    {
                        "name": "content",
                        "fields": [
                            {
                                "fields": [
                                    {
                                        "name": "interfaceEntity",
                                        "fields": [
                                            {
                                                "fields": [
                                                    {
                                                        "name": "attributes",
                                                        "fields": [
                                                            {
                                                                "fields": [
                                                                    {
                                                                        "name": "childAction",
                                                                        "string_value": "",
                                                                    },
                                                                    {
                                                                        "name": "descr",
                                                                        "string_value": "",
                                                                    },
                                                                ]
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "name": "children",
                                                        "fields": [
                                                            {
                                                                "fields": [
                                                                    {
                                                                        "name": "l1PhysIf",
                                                                        "fields": [
                                                                            {
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "attributes",
                                                                                        "fields": [
                                                                                            {
                                                                                                "fields": [
                                                                                                    {},
                                                                                                    {},
                                                                                                ]
                                                                                            }
                                                                                        ],
                                                                                    },
                                                                                    {
                                                                                        "name": "children",
                                                                                        "fields": [
                                                                                            {
                                                                                                "fields": [
                                                                                                    {}
                                                                                                ]
                                                                                            },
                                                                                            {
                                                                                                "fields": [
                                                                                                    {}
                                                                                                ]
                                                                                            },
                                                                                        ],
                                                                                    },
                                                                                ]
                                                                            }
                                                                        ],
                                                                    }
                                                                ]
                                                            },
                                                            {
                                                                "fields": [
                                                                    {
                                                                        "name": "l1PhysIf",
                                                                        "fields": [
                                                                            {
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "attributes",
                                                                                        "fields": [
                                                                                            {
                                                                                                "fields": [
                                                                                                    {},
                                                                                                    {},
                                                                                                ]
                                                                                            }
                                                                                        ],
                                                                                    },
                                                                                    {
                                                                                        "name": "children",
                                                                                        "fields": [
                                                                                            {
                                                                                                "fields": [
                                                                                                    {}
                                                                                                ]
                                                                                            },
                                                                                            {
                                                                                                "fields": [
                                                                                                    {}
                                                                                                ]
                                                                                            },
                                                                                        ],
                                                                                    },
                                                                                ]
                                                                            }
                                                                        ],
                                                                    }
                                                                ]
                                                            },
                                                        ],
                                                    },
                                                ]
                                            }
                                        ],
                                    }
                                ]
                            }
                        ],
                    },
                ]
            }
        ],
    }
]

NX_OPEN_DATA = [
    {
        "node_id_str": "n9k",
        "subscription_id_str": "3",
        "encoding_path": "openconfig-interfaces:interfaces",
        "collection_id": "8250",
        "msg_timestamp": "1586189156482",
        "data_gpbkv": [
            {
                "fields": [
                    {
                        "name": "keys",
                        "fields": [
                            {
                                "name": "openconfig-interfaces:interfaces",
                                "string_value": "openconfig-interfaces:interfaces",
                            }
                        ],
                    },
                    {
                        "name": "content",
                        "fields": [
                            {
                                "fields": [
                                    {
                                        "name": "interfaces",
                                        "fields": [
                                            {
                                                "fields": [
                                                    {
                                                        "name": "xmlns",
                                                        "string_value": "http://openconfig.net/yang/interfaces",
                                                    },
                                                    {
                                                        "name": "interface",
                                                        "fields": [
                                                            {
                                                                "fields": [
                                                                    {
                                                                        "name": "config",
                                                                        "fields": [
                                                                            {
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "enabled",
                                                                                        "string_value": "false",
                                                                                    },
                                                                                    {
                                                                                        "name": "mtu",
                                                                                        "uint64_value": "1500",
                                                                                    },
                                                                                ]
                                                                            }
                                                                        ],
                                                                    },
                                                                    {
                                                                        "name": "hold-time",
                                                                        "fields": [
                                                                            {
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "config",
                                                                                        "fields": [
                                                                                            {
                                                                                                "fields": [
                                                                                                    {}
                                                                                                ]
                                                                                            }
                                                                                        ],
                                                                                    },
                                                                                    {
                                                                                        "name": "state",
                                                                                        "fields": [
                                                                                            {
                                                                                                "fields": [
                                                                                                    {}
                                                                                                ]
                                                                                            }
                                                                                        ],
                                                                                    },
                                                                                ]
                                                                            }
                                                                        ],
                                                                    },
                                                                ]
                                                            },
                                                            {
                                                                "fields": [
                                                                    {
                                                                        "name": "config",
                                                                        "fields": [
                                                                            {
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "enabled",
                                                                                        "string_value": "false",
                                                                                    },
                                                                                    {
                                                                                        "name": "mtu",
                                                                                        "uint64_value": "1500",
                                                                                    },
                                                                                ]
                                                                            }
                                                                        ],
                                                                    },
                                                                    {
                                                                        "name": "hold-time",
                                                                        "fields": [
                                                                            {
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "config",
                                                                                        "fields": [
                                                                                            {
                                                                                                "fields": [
                                                                                                    {}
                                                                                                ]
                                                                                            }
                                                                                        ],
                                                                                    },
                                                                                    {
                                                                                        "name": "state",
                                                                                        "fields": [
                                                                                            {
                                                                                                "fields": [
                                                                                                    {}
                                                                                                ]
                                                                                            }
                                                                                        ],
                                                                                    },
                                                                                ]
                                                                            }
                                                                        ],
                                                                    },
                                                                ]
                                                            },
                                                        ],
                                                    },
                                                ]
                                            }
                                        ],
                                    }
                                ]
                            }
                        ],
                    },
                ]
            }
        ],
    }
]

NX_SHOW_DATA = [
    {
        "node_id_str": "n9k",
        "subscription_id_str": "5",
        "encoding_path": "show interface",
        "collection_id": "57415",
        "msg_timestamp": "1588610461076",
        "data_gpbkv": [
            {
                "fields": [
                    {
                        "name": "keys",
                        "fields": [
                            {"name": "show interface", "string_value": "show interface"}
                        ],
                    },
                    {
                        "name": "content",
                        "fields": [
                            {
                                "fields": [
                                    {
                                        "name": "TABLE_interface",
                                        "fields": [
                                            {
                                                "fields": [
                                                    {
                                                        "name": "ROW_interface",
                                                        "fields": [
                                                            {
                                                                "fields": [
                                                                    {
                                                                        "name": "interface",
                                                                        "string_value": "mgmt0",
                                                                    },
                                                                    {
                                                                        "name": "state",
                                                                        "string_value": "up",
                                                                    },
                                                                ]
                                                            },
                                                            {
                                                                "fields": [
                                                                    {
                                                                        "name": "interface",
                                                                        "string_value": "Ethernet1/1",
                                                                    },
                                                                    {
                                                                        "name": "state",
                                                                        "string_value": "down",
                                                                    },
                                                                ]
                                                            },
                                                        ],
                                                    }
                                                ]
                                            }
                                        ],
                                    }
                                ]
                            }
                        ],
                    },
                ]
            }
        ],
    }
]

CISCO_ELEMENTS_DATA = [
    {
        "node_id_str": "r14.labxtx01.us.bb",
        "subscription_id_str": "gtat-state-pmacct",
        "encoding_path": "Cisco-IOS-XR-nto-misc-oper:memory-summary/nodes/node/summary",
        "collection_id": "11",
        "collection_start_time": "1590790951700",
        "msg_timestamp": "1590790951707",
        "collection_end_time": "1590790951716",
        "content": {
            "page-size": 4096,
            "ram-memory": "28757590016",
            "free-physical-memory": "21408897024",
            "system-ram-memory": "28757590016",
            "free-application-memory": "21408897024",
            "image-memory": "4194304",
            "boot-ram-size": "0",
            "reserved-memory": "0",
            "io-memory": "0",
            "flash-system": "0",
        },
        "keys": {"node-name": "0/RP0/CPU0"},
    },
    {
        "node_id_str": "r14.labxtx01.us.bb",
        "subscription_id_str": "gtat-state-pmacct",
        "encoding_path": "Cisco-IOS-XR-qos-ma-oper:qos/nodes/node/policy-map/interface-table/interface/member-interfaces/member-interface/input/service-policy-names/service-policy-instance/statistics",
        "collection_id": "80118",
        "collection_start_time": "1561673093246",
        "msg_timestamp": "1561673093246",
        "collection_end_time": "1561673093365",
        "content": {
            "policy-name": "ntt-cos-in",
            "subscriber-group": "",
            "state": "active",
            "state-description": "",
            "class-stats": [
                {
                    "counter-validity-bitmask": "2101249",
                    "class-name": "ntp-limit",
                    "queue-descr": "",
                    "cac-state": "unknown",
                    "general-stats": {},
                    "police-stats-array": {},
                },
                {
                    "counter-validity-bitmask": "2097155",
                    "class-name": "control-q",
                    "queue-descr": "",
                    "cac-state": "unknown",
                    "general-stats": {},
                },
            ],
            "satid": 0,
        },
        "keys": {
            "node-name": "0/RP0/CPU0",
            "interface-name": ["Bundle-Ether6", "HundredGigE0/3/0/3"],
            "service-policy-name": "ntt-cos-in",
        },
    },
]


NX_DATA = [x for y in [NX_OPEN_DATA, NX_API_DATA, NX_SHOW_DATA] for x in y]

GRPC_DATA = [x for y in [JSON_DATA, KV_DATA, NX_DATA] for x in y]

GPBKV_DATA = [x for y in [KV_DATA, NX_DATA] for x in y]


def metric_cisco_grpc_gpb(cisco_grpc_gpb):
    cisco_grpc_gpb.node_id
    cisco_grpc_gpb.subscription_id
    cisco_grpc_gpb.path
    cisco_grpc_gpb.collection_id
    cisco_grpc_gpb.collection_start_time
    # cisco_grpc_gpb.msg_timestamp
    # cisco_grpc_gpb.content
    cisco_grpc_gpb.collection_end_time
    cisco_grpc_gpb.data_json
    cisco_grpc_gpb.data_gpbkv
    cisco_grpc_gpb.data
    cisco_grpc_gpb.module


def metric_cisco_grpc_gpb_nx(cisco_grpc_gpb):
    cisco_grpc_gpb.node_id
    cisco_grpc_gpb.subscription_id
    cisco_grpc_gpb.path
    cisco_grpc_gpb.collection_id
    # cisco_grpc_gpb.collection_start_time
    cisco_grpc_gpb.msg_timestamp
    # cisco_grpc_gpb.content
    # cisco_grpc_gpb.collection_end_time
    cisco_grpc_gpb.data_json
    cisco_grpc_gpb.data_gpbkv
    cisco_grpc_gpb.data
    cisco_grpc_gpb.module


def metric_cisco_element(metric):
    metric.node_id
    metric.subscription_id
    metric.path
    metric.collection_id
    metric.collection_start_time
    metric.msg_timestamp
    metric.content
    metric.collection_end_time
    metric.data
    metric.keys
    metric.module
    assert isinstance(metric.content, dict)


def metric_test_gpvkv(cisco_gpbkv):
    cisco_gpbkv.node_id
    cisco_gpbkv.subscription_id
    cisco_gpbkv.path
    cisco_gpbkv.collection_id
    cisco_gpbkv.collection_start_time
    # cisco_gpbkv.msg_timestamp
    cisco_gpbkv.content
    cisco_gpbkv.collection_end_time
    cisco_gpbkv.module


def metric_test_grpv_nx(cisco_gpbkv):
    cisco_gpbkv.node_id
    cisco_gpbkv.subscription_id
    cisco_gpbkv.path
    cisco_gpbkv.collection_id
    # cisco_gpbkv.collection_start_time
    cisco_gpbkv.msg_timestamp
    cisco_gpbkv.content
    # cisco_gpbkv.collection_end_time
    cisco_gpbkv.module


def metric_cisco_element_nx(metric):
    assert isinstance(metric.content, dict)
    metric.node_id
    metric.subscription_id
    metric.path
    metric.collection_id
    # metric.collection_start_time
    metric.msg_timestamp
    metric.content
    # metric.collection_end_time
    metric.data
    metric.keys
    metric.module


class TestBasicCiscoGrpcGPB(AbstractTestMetric):
    CLS = CiscoGrpcGPB
    metrics = GRPC_DATA
    mandatory_property = ["data", "node_id", "subscription_id", "path", "collection_id"]


class TestBasicCiscoGrpcJson(AbstractTestMetric):
    CLS = CiscoGrpcJson
    metrics = JSON_DATA
    mandatory_property = [
        "content",
        "data",
        "node_id",
        "subscription_id",
        "path",
        "collection_id",
    ]


# @pytest.fixture(params=GPBKV_DATA)
# def gpbkv_metric(request):
#    return CiscoGrpcKV(request.param)


class TestBasicCiscoGrpcKV(AbstractTestMetric):
    CLS = CiscoGrpcKV
    metrics = GPBKV_DATA
    mandatory_property = [
        "content",
        "data",
        "node_id",
        "subscription_id",
        "path",
        "collection_id",
    ]


class TestBasicNxGrpcGPB(AbstractTestMetric):
    CLS = NxGrpcGPB
    metrics = NX_DATA
    mandatory_property = [
        "data",
        "node_id",
        "subscription_id",
        "path",
        "collection_id",
        "content",
    ]


class TestCiscoElement(AbstractTestMetric):
    CLS = CiscoElement
    metrics = CISCO_ELEMENTS_DATA
    mandatory_property = ["collection_id", "path", "data", "keys", "content"]


class TestCiscoGrpcGPB:
    """
    Collection of tests for Raw and Cisco Grpc metrics
    """

    @pytest.mark.parametrize("file_name", CISCO_FILES)
    def test_creation(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)


class TestCiscoJSON:
    @pytest.mark.parametrize("file_name", [x for x in CISCO_FILES if "json" in str(x)])
    def test_creation(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue

            # create raw metric
            raw_metric = GrpcRaw.from_base64(line)
            raw_metric.content

            # get  cisco_grpc
            cisco_grpc_gpb_metric = GrpcRawJsonToCiscoGrpcGPB().convert(raw_metric)
            try:
                encoding = cisco_grpc_gpb_metric.infer_encoding()
            except EncodingNotFound:
                encoding = None
            if encoding and encoding != CiscoEncodings.JSON:
                pytest.fail(f"File {file_name} includes a line that is not GPBVK")
            metric_cisco_grpc_gpb(cisco_grpc_gpb_metric)
            cisco_json = CiscoGrpcGPBToCiscoGrpcJson().convert(cisco_grpc_gpb_metric)
            # print(reduce_dict(cisco_grpc_gpb_metric.content, 23))
            # breakpoint()
            cisco_json.node_id
            cisco_json.subscription_id
            cisco_json.path
            cisco_json.collection_id
            cisco_json.collection_start_time
            # cisco_json.msg_timestamp
            cisco_json.content
            cisco_json.collection_end_time
            assert cisco_json.content == cisco_json.data["data_json"]
            for element in CiscoGrpcJsonToCiscoElement().transform(cisco_json):
                metric_cisco_element(element)


class TestCiscoGPVKV:
    """
    Collection of tests for cisco metrics
    """

    # def test_cisco_grpc_mandatory(self, gpbkv_metric):
    #     failed_attributes = check_metric_properties(gpbkv_metric, CISCO_GPVKV_MANDATORY)
    #     assert not failed_attributes

    # def test_cisco_grpc_basic(self, gpbkv_metric):
    #     failed_attributes = check_basic_properties(gpbkv_metric)
    #     assert not failed_attributes

    @pytest.mark.parametrize("file_name", [x for x in CISCO_FILES if "gpbkv" in str(x)])
    def test_creation(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)

            cisco_grpc_gpb_metric = GrpcRawGPBToCiscoGrpcGPB().convert(raw_metric)

            try:
                encoding = cisco_grpc_gpb_metric.infer_encoding()
            except EncodingNotFound:
                encoding = None
            if encoding and encoding != CiscoEncodings.GPBVK:
                pytest.fail(f"File {file_name} includes a line that is not GPBVK")
            metric_cisco_grpc_gpb(cisco_grpc_gpb_metric)
            cisco_gpbkv = CiscoGrpcGPBToCiscoGrpcKV().convert(cisco_grpc_gpb_metric)
            assert cisco_gpbkv.content == cisco_gpbkv.data["data_gpbkv"]
            metric_test_gpvkv(cisco_gpbkv)
            for element in PivotingCiscoGPBKVDict(element_class=CiscoElement).transform(cisco_gpbkv):
                assert element.path
                assert element.keys is not None
                metric_cisco_element(element)
                assert isinstance(element.content, dict)


class TestCiscoNX:
    @pytest.mark.parametrize("file_name", [x for x in CISCO_FILES if "nx" in str(x)])
    def test_creation_nx_general(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)

            cisco_grpc_gpb_metric = GrpcRawToNxGrpcGPB().convert(raw_metric)
            metric_cisco_grpc_gpb_nx(cisco_grpc_gpb_metric)
            try:
                encoding = cisco_grpc_gpb_metric.infer_encoding()
            except EncodingNotFound:
                encoding = None
            if encoding and encoding != CiscoEncodings.GPBVK:
                pytest.fail(f"File {file_name} includes a line that is not GPBVK")

            cisco_nx_gpbkv = NxGrpcGPBToNXGrpcKV().convert(cisco_grpc_gpb_metric)
            metric_test_grpv_nx(cisco_nx_gpbkv)

            nx_api = False
            if "api" in str(file_name):
                nx_api = True

            assert cisco_nx_gpbkv.content == cisco_nx_gpbkv.data["data_gpbkv"]
            assert cisco_nx_gpbkv.subscription_id

            for nx_elmement in PivotingCiscoGPBKVDict(element_class=CiscoElement).transform(cisco_nx_gpbkv):
                metric_cisco_element_nx(nx_elmement)

                if "api" in str(file_name):
                    nx_elmement = CiscoElementToNXElement().convert(nx_elmement)

                    assert isinstance(nx_elmement, NXElement)

                else:
                    assert isinstance(nx_elmement, CiscoElement)
                assert nx_elmement.path
                assert nx_elmement.keys is not None
                print(nx_elmement.path)
                paths = nx_elmement.sensor_paths

                # Do an extra pivot if we have an api
                if isinstance(nx_elmement, NXElement):
                    warnings = []
                    for nx_api_element in PivotingNXApiDict().transform(
                        nx_elmement, warnings
                    ):
                        metric_cisco_element_nx(nx_api_element)
                        assert isinstance(nx_api_element, CiscoElement)
                        paths = nx_api_element.sensor_paths
                        for path in paths:
                            if "children" in path:
                                pytest.fail(f"Found children in path {path}")
                    assert not warnings
