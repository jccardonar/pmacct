import pytest
from cisco_gbpvk_tools import cisco_gpbvkv

gpvb_test_cases = [
    # empty case
    {"original": {}, "expected": {}},
    # simple case with different options
    {
        "original": {
            "timestamp": "1561673093045",
            "fields": [
                {
                    "name": "keys",
                    "fields": [{"name": "node-name", "string_value": "0/RP0/CPU0"}],
                },
                {
                    "name": "content",
                    "fields": [
                        {"name": "page-size", "uint32_value": 4096},
                        {"name": "ram-memory", "uint64_value": "28758966272"},
                        {"name": "free-physical-memory", "uint64_value": "19664310272"},
                        {"name": "system-ram-memory", "uint64_value": "28758966272"},
                        {
                            "name": "free-application-memory",
                            "uint64_value": "19219832832",
                        },
                        {"name": "image-memory", "uint64_value": "4194304"},
                        {"name": "boot-ram-size", "uint64_value": "0"},
                        {"name": "reserved-memory", "uint64_value": "0"},
                        {"name": "io-memory", "uint64_value": "0"},
                        {"name": "flash-system", "uint64_value": "0"},
                    ],
                },
            ],
        },
        "expected": {
            "timestamp": "1561673093045",
            "keys": {"node-name": "0/RP0/CPU0"},
            "content": {
                "page-size": 4096,
                "ram-memory": "28758966272",
                "free-physical-memory": "19664310272",
                "system-ram-memory": "28758966272",
                "free-application-memory": "19219832832",
                "image-memory": "4194304",
                "boot-ram-size": "0",
                "reserved-memory": "0",
                "io-memory": "0",
                "flash-system": "0",
            },
        },
    },
    # keys
    {
        "original": {
            "timestamp": "1561673093045",
            "fields": [
                {
                    "name": "keys",
                    "fields": [
                        {"name": "node-name", "string_value": "0/RP0/CPU0"},
                        {"name": "node-name", "string_value": "0/RP0/CPU1"},
                    ],
                },
                {
                    "name": "content",
                    "fields": [
                        {"name": "page-size", "uint32_value": 4096},
                        {"name": "ram-memory", "uint64_value": "28758966272"},
                        {"name": "free-physical-memory", "uint64_value": "19664310272"},
                        {"name": "system-ram-memory", "uint64_value": "28758966272"},
                        {
                            "name": "free-application-memory",
                            "uint64_value": "19219832832",
                        },
                        {"name": "image-memory", "uint64_value": "4194304"},
                        {"name": "boot-ram-size", "uint64_value": "0"},
                        {"name": "reserved-memory", "uint64_value": "0"},
                        {"name": "io-memory", "uint64_value": "0"},
                        {"name": "flash-system", "uint64_value": "0"},
                    ],
                },
            ],
        },
        "expected": {
            "timestamp": "1561673093045",
            "keys": {"node-name": ["0/RP0/CPU0", "0/RP0/CPU1"]},
            "content": {
                "page-size": 4096,
                "ram-memory": "28758966272",
                "free-physical-memory": "19664310272",
                "system-ram-memory": "28758966272",
                "free-application-memory": "19219832832",
                "image-memory": "4194304",
                "boot-ram-size": "0",
                "reserved-memory": "0",
                "io-memory": "0",
                "flash-system": "0",
            },
        },
    },
    #
    {
        "original": {
            "timestamp": "1561673093381",
            "fields": [
                {
                    "name": "keys",
                    "fields": [
                        {"name": "node-name", "string_value": "0/RP0/CPU0"},
                        {"name": "interface-name", "string_value": "Bundle-Ether6"},
                        {
                            "name": "interface-name",
                            "string_value": "HundredGigE0/3/0/3",
                        },
                        {"name": "service-policy-name", "string_value": "ntt-cos-out"},
                    ],
                },
                {
                    "name": "content",
                    "fields": [
                        {"name": "policy-name", "string_value": "ntt-cos-out"},
                        {"name": "subscriber-group", "string_value": ""},
                        {"name": "state", "string_value": "active"},
                        {"name": "state-description", "string_value": ""},
                        {
                            "name": "class-stats",
                            "fields": [
                                {
                                    "name": "counter-validity-bitmask",
                                    "uint64_value": "4783898624",
                                },
                                {"name": "class-name", "string_value": "control-q"},
                                {"name": "queue-descr", "string_value": ""},
                                {"name": "cac-state", "string_value": "unknown"},
                                {
                                    "name": "general-stats",
                                    "fields": [
                                        {
                                            "name": "transmit-packets",
                                            "uint64_value": "710637",
                                        },
                                        {
                                            "name": "transmit-bytes",
                                            "uint64_value": "64380823",
                                        },
                                        {
                                            "name": "total-drop-packets",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "total-drop-bytes",
                                            "uint64_value": "0",
                                        },
                                        {"name": "total-drop-rate", "uint32_value": 0},
                                        {"name": "match-data-rate", "uint32_value": 0},
                                        {
                                            "name": "total-transmit-rate",
                                            "uint32_value": 0,
                                        },
                                        {
                                            "name": "pre-policy-matched-packets",
                                            "uint64_value": "710632",
                                        },
                                        {
                                            "name": "pre-policy-matched-bytes",
                                            "uint64_value": "64380315",
                                        },
                                    ],
                                },
                                {
                                    "name": "queue-stats-array",
                                    "fields": [
                                        {"name": "queue-id", "uint32_value": 524610},
                                        {
                                            "name": "tail-drop-packets",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "tail-drop-bytes",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "queue-instance-length",
                                            "fields": [
                                                {"name": "value", "uint32_value": 0},
                                                {
                                                    "name": "unit",
                                                    "string_value": "policy-param-unit-packets",
                                                },
                                            ],
                                        },
                                        {
                                            "name": "queue-drop-threshold",
                                            "uint32_value": 0,
                                        },
                                        {
                                            "name": "forced-wred-stats-display",
                                            "string_value": "true",
                                        },
                                        {
                                            "name": "random-drop-packets",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "random-drop-bytes",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "max-threshold-packets",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "max-threshold-bytes",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "conform-packets",
                                            "uint64_value": "710637",
                                        },
                                        {
                                            "name": "conform-bytes",
                                            "uint64_value": "64380823",
                                        },
                                        {"name": "exceed-packets", "uint64_value": "0"},
                                        {"name": "exceed-bytes", "uint64_value": "0"},
                                        {"name": "conform-rate", "uint32_value": 0},
                                        {"name": "exceed-rate", "uint32_value": 0},
                                    ],
                                },
                                {
                                    "name": "wred-stats-array",
                                    "fields": [
                                        {
                                            "name": "profile-title",
                                            "string_value": "Default WRED Curve",
                                        },
                                        {
                                            "name": "red-label",
                                            "fields": [
                                                {
                                                    "name": "wred-type",
                                                    "string_value": "red-with-default-min-max",
                                                },
                                                {"name": "value", "uint32_value": 0},
                                            ],
                                        },
                                        {
                                            "name": "red-transmit-packets",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "red-transmit-bytes",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "random-drop-packets",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "random-drop-bytes",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "max-threshold-packets",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "max-threshold-bytes",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "red-ecn-marked-packets",
                                            "uint64_value": "0",
                                        },
                                        {
                                            "name": "red-ecn-marked-bytes",
                                            "uint64_value": "0",
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        },
        "expected": {
            "timestamp": "1561673093381",
            "keys": {
                "node-name": "0/RP0/CPU0",
                "interface-name": ["Bundle-Ether6", "HundredGigE0/3/0/3"],
                "service-policy-name": "ntt-cos-out",
            },
            "content": {
                "policy-name": "ntt-cos-out",
                "subscriber-group": "",
                "state": "active",
                "state-description": "",
                "class-stats": {
                    "counter-validity-bitmask": "4783898624",
                    "class-name": "control-q",
                    "queue-descr": "",
                    "cac-state": "unknown",
                    "general-stats": {
                        "transmit-packets": "710637",
                        "transmit-bytes": "64380823",
                        "total-drop-packets": "0",
                        "total-drop-bytes": "0",
                        "total-drop-rate": 0,
                        "match-data-rate": 0,
                        "total-transmit-rate": 0,
                        "pre-policy-matched-packets": "710632",
                        "pre-policy-matched-bytes": "64380315",
                    },
                    "queue-stats-array": {
                        "queue-id": 524610,
                        "tail-drop-packets": "0",
                        "tail-drop-bytes": "0",
                        "queue-instance-length": {
                            "value": 0,
                            "unit": "policy-param-unit-packets",
                        },
                        "queue-drop-threshold": 0,
                        "forced-wred-stats-display": "true",
                        "random-drop-packets": "0",
                        "random-drop-bytes": "0",
                        "max-threshold-packets": "0",
                        "max-threshold-bytes": "0",
                        "conform-packets": "710637",
                        "conform-bytes": "64380823",
                        "exceed-packets": "0",
                        "exceed-bytes": "0",
                        "conform-rate": 0,
                        "exceed-rate": 0,
                    },
                    "wred-stats-array": {
                        "profile-title": "Default WRED Curve",
                        "red-label": {
                            "wred-type": "red-with-default-min-max",
                            "value": 0,
                        },
                        "red-transmit-packets": "0",
                        "red-transmit-bytes": "0",
                        "random-drop-packets": "0",
                        "random-drop-bytes": "0",
                        "max-threshold-packets": "0",
                        "max-threshold-bytes": "0",
                        "red-ecn-marked-packets": "0",
                        "red-ecn-marked-bytes": "0",
                    },
                },
            },
        },
    },
    # nx case. a bit large. We have to make sure Unknowns do not appear.
    {
        "original": {
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
                                                                {
                                                                    "name": "dn",
                                                                    "string_value": "sys/intf",
                                                                },
                                                                {
                                                                    "name": "modTs",
                                                                    "string_value": "2020-04-04T20:16:50.164+00:00",
                                                                },
                                                                {
                                                                    "name": "persistentOnReload",
                                                                    "string_value": "true",
                                                                },
                                                                {
                                                                    "name": "status",
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
                                                                                                {
                                                                                                    "name": "FECMode",
                                                                                                    "string_value": "auto",
                                                                                                },
                                                                                                {
                                                                                                    "name": "accessVlan",
                                                                                                    "string_value": "vlan-1",
                                                                                                },
                                                                                                {
                                                                                                    "name": "adminSt",
                                                                                                    "string_value": "down",
                                                                                                },
                                                                                                {
                                                                                                    "name": "autoNeg",
                                                                                                    "string_value": "on",
                                                                                                },
                                                                                                {
                                                                                                    "name": "beacon",
                                                                                                    "string_value": "off",
                                                                                                },
                                                                                                {
                                                                                                    "name": "bw",
                                                                                                    "uint32_value": 0,
                                                                                                },
                                                                                                {
                                                                                                    "name": "childAction",
                                                                                                    "string_value": "",
                                                                                                },
                                                                                                {
                                                                                                    "name": "controllerId",
                                                                                                    "string_value": "",
                                                                                                },
                                                                                                {
                                                                                                    "name": "delay",
                                                                                                    "uint32_value": 1,
                                                                                                },
                                                                                                {
                                                                                                    "name": "descr",
                                                                                                    "string_value": "",
                                                                                                },
                                                                                                {
                                                                                                    "name": "dot1qEtherType",
                                                                                                    "string_value": "0x8100",
                                                                                                },
                                                                                                {
                                                                                                    "name": "duplex",
                                                                                                    "string_value": "auto",
                                                                                                },
                                                                                                {
                                                                                                    "name": "ethpmCfgFailedBmp",
                                                                                                    "string_value": "",
                                                                                                },
                                                                                                {
                                                                                                    "name": "ethpmCfgFailedTs",
                                                                                                    "string_value": "00:00:00:00.000",
                                                                                                },
                                                                                                {
                                                                                                    "name": "ethpmCfgState",
                                                                                                    "uint32_value": 0,
                                                                                                },
                                                                                                {
                                                                                                    "name": "id",
                                                                                                    "string_value": "eth1/71",
                                                                                                },
                                                                                                {
                                                                                                    "name": "inhBw",
                                                                                                    "uint32_value": 2147483647,
                                                                                                },
                                                                                                {
                                                                                                    "name": "layer",
                                                                                                    "string_value": "Layer2",
                                                                                                },
                                                                                                {
                                                                                                    "name": "linkDebounce",
                                                                                                    "uint32_value": 100,
                                                                                                },
                                                                                                {
                                                                                                    "name": "linkDebounceLinkUp",
                                                                                                    "uint32_value": 0,
                                                                                                },
                                                                                                {
                                                                                                    "name": "linkLog",
                                                                                                    "string_value": "default",
                                                                                                },
                                                                                                {
                                                                                                    "name": "linkTransmitReset",
                                                                                                    "string_value": "enable",
                                                                                                },
                                                                                                {
                                                                                                    "name": "mdix",
                                                                                                    "string_value": "auto",
                                                                                                },
                                                                                                {
                                                                                                    "name": "medium",
                                                                                                    "string_value": "broadcast",
                                                                                                },
                                                                                                {
                                                                                                    "name": "modTs",
                                                                                                    "string_value": "2020-04-04T20:20:40.448+00:00",
                                                                                                },
                                                                                                {
                                                                                                    "name": "mode",
                                                                                                    "string_value": "access",
                                                                                                },
                                                                                                {
                                                                                                    "name": "mtu",
                                                                                                    "uint32_value": 1500,
                                                                                                },
                                                                                                {
                                                                                                    "name": "name",
                                                                                                    "string_value": "",
                                                                                                },
                                                                                                {
                                                                                                    "name": "nativeVlan",
                                                                                                    "string_value": "vlan-1",
                                                                                                },
                                                                                                {
                                                                                                    "name": "packetTimestampEgressSourceId",
                                                                                                    "uint32_value": 0,
                                                                                                },
                                                                                                {
                                                                                                    "name": "packetTimestampIngressSourceId",
                                                                                                    "uint32_value": 0,
                                                                                                },
                                                                                                {
                                                                                                    "name": "packetTimestampState",
                                                                                                    "string_value": "disable",
                                                                                                },
                                                                                                {
                                                                                                    "name": "persistentOnReload",
                                                                                                    "string_value": "true",
                                                                                                },
                                                                                                {
                                                                                                    "name": "portT",
                                                                                                    "string_value": "leaf",
                                                                                                },
                                                                                                {
                                                                                                    "name": "rn",
                                                                                                    "string_value": "phys-[eth1/71]",
                                                                                                },
                                                                                                {
                                                                                                    "name": "routerMac",
                                                                                                    "string_value": "not-applicable",
                                                                                                },
                                                                                                {
                                                                                                    "name": "snmpTrapSt",
                                                                                                    "string_value": "enable",
                                                                                                },
                                                                                                {
                                                                                                    "name": "spanMode",
                                                                                                    "string_value": "not-a-span-dest",
                                                                                                },
                                                                                                {
                                                                                                    "name": "speed",
                                                                                                    "string_value": "auto",
                                                                                                },
                                                                                                {
                                                                                                    "name": "speedGroup",
                                                                                                    "string_value": "auto",
                                                                                                },
                                                                                                {
                                                                                                    "name": "status",
                                                                                                    "string_value": "",
                                                                                                },
                                                                                                {
                                                                                                    "name": "switchingSt",
                                                                                                    "string_value": "disabled",
                                                                                                },
                                                                                                {
                                                                                                    "name": "trunkLog",
                                                                                                    "string_value": "default",
                                                                                                },
                                                                                                {
                                                                                                    "name": "trunkVlans",
                                                                                                    "string_value": "1-4094",
                                                                                                },
                                                                                                {
                                                                                                    "name": "usage",
                                                                                                    "string_value": "discovery",
                                                                                                },
                                                                                                {
                                                                                                    "name": "userCfgdFlags",
                                                                                                    "string_value": "",
                                                                                                },
                                                                                                {
                                                                                                    "name": "vlanmgrCfgFailedBmp",
                                                                                                    "string_value": "",
                                                                                                },
                                                                                                {
                                                                                                    "name": "vlanmgrCfgFailedTs",
                                                                                                    "string_value": "00:00:00:00.000",
                                                                                                },
                                                                                                {
                                                                                                    "name": "vlanmgrCfgState",
                                                                                                    "uint32_value": 0,
                                                                                                },
                                                                                                {
                                                                                                    "name": "voicePortCos",
                                                                                                    "uint32_value": 4294967295,
                                                                                                },
                                                                                                {
                                                                                                    "name": "voicePortTrust",
                                                                                                    "uint32_value": 4294967295,
                                                                                                },
                                                                                                {
                                                                                                    "name": "voiceVlanId",
                                                                                                    "uint32_value": 0,
                                                                                                },
                                                                                                {
                                                                                                    "name": "voiceVlanType",
                                                                                                    "string_value": "none",
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
                                                                                                    "name": "rmonDot3Stats",
                                                                                                    "fields": [
                                                                                                        {
                                                                                                            "fields": [
                                                                                                                {
                                                                                                                    "name": "attributes",
                                                                                                                    "fields": [
                                                                                                                        {
                                                                                                                            "fields": [
                                                                                                                                {
                                                                                                                                    "name": "alignmentErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "babble",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "carrierSenseErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "clearTs",
                                                                                                                                    "string_value": "never",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "controlInUnknownOpcodes",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "deferredTransmissions",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "excessiveCollisions",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "fCSErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "frameTooLongs",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "inPauseFrames",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "inputdribble",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "internalMacReceiveErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "internalMacTransmitErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "lateCollisions",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "lostCarrierErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "multipleCollisionFrames",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "noCarrierErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "outPauseFrames",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "rn",
                                                                                                                                    "string_value": "dbgDot3Stats",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "runts",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "sQETTestErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "singleCollisionFrames",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                                {
                                                                                                                                    "name": "symbolErrors",
                                                                                                                                    "uint64_value": "0",
                                                                                                                                },
                                                                                                                            ]
                                                                                                                        }
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
                                                        }
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
        },
        "expected": {
            "keys": {"sys/intf": "sys/intf"},
            "content": {
                "interfaceEntity": {
                    "attributes": {
                        "childAction": "",
                        "descr": "",
                        "dn": "sys/intf",
                        "modTs": "2020-04-04T20:16:50.164+00:00",
                        "persistentOnReload": "true",
                        "status": "",
                    },
                    "children": {
                        "l1PhysIf": {
                            "attributes": {
                                "FECMode": "auto",
                                "accessVlan": "vlan-1",
                                "adminSt": "down",
                                "autoNeg": "on",
                                "beacon": "off",
                                "bw": 0,
                                "childAction": "",
                                "controllerId": "",
                                "delay": 1,
                                "descr": "",
                                "dot1qEtherType": "0x8100",
                                "duplex": "auto",
                                "ethpmCfgFailedBmp": "",
                                "ethpmCfgFailedTs": "00:00:00:00.000",
                                "ethpmCfgState": 0,
                                "id": "eth1/71",
                                "inhBw": 2147483647,
                                "layer": "Layer2",
                                "linkDebounce": 100,
                                "linkDebounceLinkUp": 0,
                                "linkLog": "default",
                                "linkTransmitReset": "enable",
                                "mdix": "auto",
                                "medium": "broadcast",
                                "modTs": "2020-04-04T20:20:40.448+00:00",
                                "mode": "access",
                                "mtu": 1500,
                                "name": "",
                                "nativeVlan": "vlan-1",
                                "packetTimestampEgressSourceId": 0,
                                "packetTimestampIngressSourceId": 0,
                                "packetTimestampState": "disable",
                                "persistentOnReload": "true",
                                "portT": "leaf",
                                "rn": "phys-[eth1/71]",
                                "routerMac": "not-applicable",
                                "snmpTrapSt": "enable",
                                "spanMode": "not-a-span-dest",
                                "speed": "auto",
                                "speedGroup": "auto",
                                "status": "",
                                "switchingSt": "disabled",
                                "trunkLog": "default",
                                "trunkVlans": "1-4094",
                                "usage": "discovery",
                                "userCfgdFlags": "",
                                "vlanmgrCfgFailedBmp": "",
                                "vlanmgrCfgFailedTs": "00:00:00:00.000",
                                "vlanmgrCfgState": 0,
                                "voicePortCos": 4294967295,
                                "voicePortTrust": 4294967295,
                                "voiceVlanId": 0,
                                "voiceVlanType": "none",
                            },
                            "children": {
                                "rmonDot3Stats": {
                                    "attributes": {
                                        "alignmentErrors": "0",
                                        "babble": "0",
                                        "carrierSenseErrors": "0",
                                        "clearTs": "never",
                                        "controlInUnknownOpcodes": "0",
                                        "deferredTransmissions": "0",
                                        "excessiveCollisions": "0",
                                        "fCSErrors": "0",
                                        "frameTooLongs": "0",
                                        "inPauseFrames": "0",
                                        "inputdribble": "0",
                                        "internalMacReceiveErrors": "0",
                                        "internalMacTransmitErrors": "0",
                                        "lateCollisions": "0",
                                        "lostCarrierErrors": "0",
                                        "multipleCollisionFrames": "0",
                                        "noCarrierErrors": "0",
                                        "outPauseFrames": "0",
                                        "rn": "dbgDot3Stats",
                                        "runts": "0",
                                        "sQETTestErrors": "0",
                                        "singleCollisionFrames": "0",
                                        "symbolErrors": "0",
                                    }
                                }
                            },
                        }
                    },
                }
            },
        },
    },
]


class TestPivotDict:
    @pytest.mark.parametrize(
        "original,expected", [(x["original"], x["expected"]) for x in gpvb_test_cases]
    )
    def test_pivot_dict(self, original, expected):
        pivoter = cisco_gpbvkv.PivotingCiscoGPBKVDict()
        warnings = set()
        assert pivoter.pivot_telemetry_field(original, warnings) == expected
        assert not warnings
