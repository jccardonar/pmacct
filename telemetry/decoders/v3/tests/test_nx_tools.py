import pytest
from nx_tools import nx_api

nx_test_cases = [
    {"original": {}, "expected": {}, "paths": []},
    {
        "original": {
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
        "expected": {
                "interfaceEntity": {
                    "childAction": "",
                    "descr": "",
                    "dn": "sys/intf",
                    "modTs": "2020-04-04T20:16:50.164+00:00",
                    "persistentOnReload": "true",
                    "phys-": {
                        "FECMode": "auto",
                        "accessVlan": "vlan-1",
                        "adminSt": "down",
                        "autoNeg": "on",
                        "beacon": "off",
                        "bw": 0,
                        "childAction": "",
                        "controllerId": "",
                        "dbgDot3Stats": {
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
                        },
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
                    "status": "",
            }
        },
        "paths": ["sys/intf/phys-/dbgDot3Stats", "sys/intf/phys-"],
    },
]


class TestPivotDict:
    @pytest.mark.parametrize(
        "original,expected,paths", [(x["original"], x["expected"], x["paths"]) for x in nx_test_cases]
    )
    def test_pivot_dict(self, original, expected, paths):
        pivoter = nx_api.PivotingNXApiDict()
        warnings = set()
        pivoted = pivoter.pivot_nx_api(original, warnings)
        assert pivoted == expected
        assert not warnings
