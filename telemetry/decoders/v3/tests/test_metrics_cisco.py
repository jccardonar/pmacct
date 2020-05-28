"""
Tests cisco metrics construction, transformation and operations.
"""
import pytest
from pathlib import Path
from .utils_test import data_folder, load_dump_file
from metric_types.base_types import GrpcRaw
from metric_types.cisco_metrics import (
    CiscoGrpcRaw,
    EncodingNotFound,
    CiscoEncodings,
    CiscoGrpcKV,
)

DATA_FOLDER = data_folder()
CISCO_DUMP_FOLDER = DATA_FOLDER / "cisco_dumps"
CISCO_FILES = [x for x in CISCO_DUMP_FOLDER.iterdir() if x.is_file()]


class TestCiscoGrpcRaw:
    """
    Collection of tests for Cisco Raw metrics
    """

    @pytest.mark.parametrize("file_name", CISCO_FILES)
    def test_creation(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)
            cisco_raw = CiscoGrpcRaw.from_grpc_raw_metric(raw_metric)
            try:
                encoding = cisco_raw.infer_encoding()
            except EncodingNotFound:
                encoding = None
            assert cisco_raw.subscription_id


class TestCiscoGPVKV:
    """
    Collection of tests for cisco metrics
    """

    @pytest.mark.parametrize("file_name", [x for x in CISCO_FILES if "gpbkv" in str(x)])
    def test_creation(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            raw_metric = GrpcRaw.from_base64(line)
            cisco_raw = CiscoGrpcRaw.from_grpc_raw_metric(raw_metric)
            try:
                encoding = cisco_raw.infer_encoding()
            except EncodingNotFound:
                encoding = None
            if encoding and encoding != CiscoEncodings.GPBVK:
                pytest.fail(f"File {file_name} includes a line that is not GPBVK")
            cisco_gpbvk = CiscoGrpcKV.from_cisco_grpc_raw_metric(cisco_raw)
            assert cisco_gpbvk.content == cisco_gpbvk.data["data_gpbkv"]
