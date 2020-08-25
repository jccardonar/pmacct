import pytest
from utils_test import AbstractTestMetric
from metric_types.huawei.huawei_metrics import  HuaweiGrpcGPB


HUAWEICOMPACT_EXAMPLES = [
    {
        "node_id_str": "example",
        "subscription_id_str": "DAISY",
        "sensor_path": "openconfig-interfaces:interfaces/interface/state/counters",
        "collection_id": "7562022",
        "collection_start_time": "1565030039335",
        "msg_timestamp": "1565030039435",
        "data_gpb": {
            "row": [
                {
                    "timestamp": "1565030039335",
                    "content": "Ci0aCjEwR0UxLzAvMjUiHxIdOABAABAAMAAYACAASAAoAHgAgAEAWABwAGAAaAA=",
                },
                {
                    "timestamp": "1565030039335",
                    "content": "Ci0aCjEwR0UxLzAvMjYiHxIdOABAABAAMAAYACAASAAoAHgAgAEAWABwAGAAaAA=",
                },
            ]
        },
        "collection_end_time": "1565030039355",
        "current_period": 1000,
        "except_desc": "OK",
    }
]

def pytest_generate_tests(metafunc):
    if issubclass(metafunc.cls, AbstractTestMetric):
        metrics = []
        for metric_value in metafunc.cls.metrics:
            metric = metafunc.cls.constructor(metric_value)
            metrics.append(metric)
        metafunc.parametrize("metric", metrics, scope="class")


class TestHuaweiGrpcGP(AbstractTestMetric):
    CLS = HuaweiGrpcGPB
    metrics = HUAWEICOMPACT_EXAMPLES
    mandatory_property = [
        "path",
        "node_id",
        "subscription_id",
        "collection_end_time",
        "collection_start_time",
        "msg_timestamp",
        "data_gpb",
    ]

class TestOther:
    def  test_other(self):
        assert True


