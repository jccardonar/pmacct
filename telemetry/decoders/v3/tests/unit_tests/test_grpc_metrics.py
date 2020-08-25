import pytest
from metric_types.base_types import GrpcRaw
from .utils_test import check_metric_properties, check_basic_properties


GRPC_ACCESS_TEST = [{"content": "dummycontent"}]

MANDATORY = ["content", "data"]

@pytest.fixture(params=GRPC_ACCESS_TEST)
def metric(request):
    return GrpcRaw(request.param)

class TestGrpcRaw:

    def test_grpc_raw_mandatory(self, metric):
        failed_attributes = check_metric_properties(metric, MANDATORY)
        assert not failed_attributes


    def test_grpc_raw_basic(self, metric):
        failed_attributes = check_basic_properties(metric)
        assert not failed_attributes





