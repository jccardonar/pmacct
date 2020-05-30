import pytest
from cisco_gbpvk_tools import cisco_gpbvkv

gpvb_test_cases = [
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
