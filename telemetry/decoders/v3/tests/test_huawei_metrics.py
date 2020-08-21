import pytest
from .utils_test import (
    data_folder,
    load_dump_file,
    AbstractTestMetric,
    pytest_generate_tests
)
from metric_types.huawei.huawei_metrics import (
    HuaweiGrpcGPB,
    HuaweiCompact,
    GrpcRawGPBToHuaweiGrpcGPB,
    HuaweDecoderConstructor,
    HuaweiGrpcGPBToHuaweiCompact,
    HuaweCompactToHuaweiElements,
)
from metric_types.base_types import GrpcRaw

DATA_FOLDER = data_folder()
HUAWEI_DUMP_FOLDER = DATA_FOLDER / "huawei_dumps"
HUAWEI_FILES = [x for x in HUAWEI_DUMP_FOLDER.iterdir() if x.is_file()]



HUAWEICOMPACT_EXAMPLES = [
    {
        "node_id_str": "example",
        "subscription_id_str": "DAISY",
        "sensor_path": "openconfig-interfaces:interfaces/interface/state/counters",
        "collection_id": "7562022",
        "collection_start_time": 1565030039335,
        "msg_timestamp": 1565030039435,
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


class TestBasicHuaweiGrpcGPB(AbstractTestMetric):
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


class TestBasicHuaweiCompact(AbstractTestMetric):
    CLS = HuaweiCompact
    metrics = HUAWEICOMPACT_EXAMPLES
    mandatory_property = [
        "path",
        "node_id",
        "subscription_id",
        "collection_end_time",
        "collection_start_time",
        "msg_timestamp",
        "data_gpb",
        "content"
    ]


def metric_huawei(metric):
    metric.node_id
    metric.subscription_id
    metric.path
    metric.collection_id
    metric.collection_start_time
    metric.msg_timestamp
    metric.collection_end_time
    # metric.data_json
    metric.data_gpb
    metric.module


def metric_huawei_compact(metric):
    metric.node_id
    metric.subscription_id
    metric.path
    metric.collection_id
    metric.collection_start_time
    metric.msg_timestamp
    metric.collection_end_time
    # metric.data_json
    metric.data_gpb
    metric.module
    metric.content

def metric_element(metric):
    metric.node_id
    metric.subscription_id
    metric.path
    metric.collection_id
    metric.collection_start_time
    metric.msg_timestamp
    metric.collection_end_time
    metric.module
    metric.content

#@pytest.fixture(params=HUAWEICOMPACT_EXAMPLES)
#def huawei_compact_metrics(request):
#    return HuaweiCompact(request.param)

class TestHuaweiGPB:

    @pytest.mark.parametrize("file_name", HUAWEI_FILES)
    def test_creation(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)
            huawei_gpb = GrpcRawGPBToHuaweiGrpcGPB().convert(raw_metric)
            metric_huawei(huawei_gpb)


@pytest.fixture
def huawei_decoder_map():
    decoder = {
        "huawei-ifm": {"module": "huawei_ifm_pb2", "msg": "Ifm"},
        "huawei-devm": {"module": "huawei_devm_pb2", "msg": "Devm"},
        "openconfig-interfaces": {
            "module": "openconfig_interfaces_pb2",
            "msg": "Interfaces",
        },
    }
    return decoder


@pytest.fixture
def huawei_decoder_constructor(huawei_decoder_map):
    return HuaweDecoderConstructor(huawei_decoder_map)


class TestHuaweiCompact:
    @pytest.mark.parametrize("file_name", HUAWEI_FILES)
    def test_creation(self, file_name, huawei_decoder_constructor):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)
            huawei_gpb = GrpcRawGPBToHuaweiGrpcGPB().convert(raw_metric)
            metric_huawei(huawei_gpb)
            huawe_compact = HuaweiGrpcGPBToHuaweiCompact().convert(huawei_gpb)
            metric_huawei_compact(huawe_compact)

            #decoder = huawei_decoder_constructor.get_decoder(huawe_compact.module)
            elem_decoder = HuaweCompactToHuaweiElements(huawei_decoder_constructor)
            for elem in elem_decoder.transform(huawe_compact):
                metric_element(elem)
                assert elem.content is not None

