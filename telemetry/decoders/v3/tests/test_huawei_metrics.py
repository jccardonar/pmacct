import pytest
from .utils_test import (
    data_folder,
    load_dump_file,
    check_metric_properties,
    check_basic_properties,
)
from metric_types.huawei_metrics import (
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


@pytest.fixture(params=HUAWEICOMPACT_EXAMPLES)
def grpc_metric(request):
    return HuaweiGrpcGPB(request.param)


HUAWEI_GPB_MANDATORY = [
    "path",
    "node_id",
    "subscription_id",
    "collection_end_time",
    "collection_start_time",
    "msg_timestamp",
    "data_gpb",
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

@pytest.fixture(params=HUAWEICOMPACT_EXAMPLES)
def huawei_compact_metrics(request):
    return HuaweiCompact(request.param)


class TestHuaweiGPB:
    def test_cisco_grpc_mandatory(self, grpc_metric):
        failed_attributes = check_metric_properties(grpc_metric, HUAWEI_GPB_MANDATORY)
        assert not failed_attributes

    def test_cisco_grpc_basic(self, grpc_metric):
        failed_attributes = check_basic_properties(grpc_metric)
        assert not failed_attributes

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
    def test_cisco_grpc_mandatory(self, huawei_compact_metrics):
        failed_attributes = check_metric_properties(
            huawei_compact_metrics, HUAWEI_GPB_MANDATORY
        )
        assert not failed_attributes

    def test_cisco_grpc_basic(self, huawei_compact_metrics):
        failed_attributes = check_basic_properties(huawei_compact_metrics)
        assert not failed_attributes

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

            decoder = huawei_decoder_constructor.get_decoder(huawe_compact.module)
            elem_decoder = HuaweCompactToHuaweiElements(decoder)
            for elem in elem_decoder.transform(huawe_compact):
                metric_element(elem)
                assert elem.content is not None

