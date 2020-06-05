"""
Tests cisco metrics construction, transformation and operations.
"""
import pytest
from pathlib import Path
from .utils_test import data_folder, load_dump_file
from metric_types.base_types import GrpcRaw
from metric_types.cisco_metrics import (
    CiscoGrpcGPB,
    EncodingNotFound,
    CiscoEncodings,
    CiscoGrpcKV,
    CiscoGrpcJson,
    NxGrpcGPB,
    NXGrpcKV,
    NXElement
)

DATA_FOLDER = data_folder()
CISCO_DUMP_FOLDER = DATA_FOLDER / "cisco_dumps"
CISCO_FILES = [x for x in CISCO_DUMP_FOLDER.iterdir() if x.is_file()]


class TestCiscoGrpcGPB:
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

class TestCiscoJSON:
    @pytest.mark.parametrize("file_name", [x for x in CISCO_FILES if "json" in str(x)])
    def test_creation(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)
            cisco_raw = CiscoGrpcGPB.from_grpc_msg_json(raw_metric)
            try:
                encoding = cisco_raw.infer_encoding()
            except EncodingNotFound:
                encoding = None
            if encoding and encoding != CiscoEncodings.JSON:
                pytest.fail(f"File {file_name} includes a line that is not GPBVK")
            cisco_json = CiscoGrpcJson.from_cisco_grpc_gpb(cisco_raw)
            assert cisco_json.content == cisco_json.data["data_json"]
            assert cisco_json.subscription_id
            for cisco_elmement in cisco_json.get_elements():
                print(cisco_elmement.path, cisco_elmement.keys)



class TestCiscoGPVKV:
    """
    Collection of tests for cisco metrics
    """

    @pytest.mark.parametrize("file_name", [x for x in CISCO_FILES if "gpbkv" in str(x)])
    def test_creation(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)
            cisco_raw = CiscoGrpcGPB.from_grpc_msg_gpb(raw_metric)
            try:
                encoding = cisco_raw.infer_encoding()
            except EncodingNotFound:
                encoding = None
            if encoding and encoding != CiscoEncodings.GPBVK:
                pytest.fail(f"File {file_name} includes a line that is not GPBVK")
            cisco_gpbvk = CiscoGrpcKV.from_cisco_grpc_gpb(cisco_raw)
            assert cisco_gpbvk.content == cisco_gpbvk.data["data_gpbkv"]
            assert cisco_gpbvk.subscription_id
            for cisco_elmement in cisco_gpbvk.pivot_data():
                assert cisco_elmement.path
                assert cisco_elmement.keys is not None

class TestCiscoNX:

    @pytest.mark.parametrize("file_name", [x for x in CISCO_FILES if "nx" in str(x)])
    def test_creation_nx_general(self, file_name):
        content = load_dump_file(file_name).split("\n")
        for line in content:
            if not line:
                continue
            raw_metric = GrpcRaw.from_base64(line)
            cisco_nx_raw = NxGrpcGPB.from_grpc_msg_gpb(raw_metric)
            try:
                encoding = cisco_nx_raw.infer_encoding()
            except EncodingNotFound:
                encoding = None
            if encoding and encoding != CiscoEncodings.GPBVK:
                pytest.fail(f"File {file_name} includes a line that is not GPBVK")
            cisco_nx_gpbvk = NXGrpcKV.from_cisco_grpc_gpb(cisco_nx_raw)
            assert cisco_nx_gpbvk.content == cisco_nx_gpbvk.data["data_gpbkv"]
            assert cisco_nx_gpbvk.subscription_id
            for nx_elmement in cisco_nx_gpbvk.pivot_data():
                assert isinstance(nx_elmement, NXElement)
                assert nx_elmement.path
                assert nx_elmement.keys is not None
                print(nx_elmement.path)
                if "sys" in nx_elmement.path and "qos" not in nx_elmement.path:
                    breakpoint()



