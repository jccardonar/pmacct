from pathlib import Path
from typing import Union
from metric_types.base_types import AttrNotFound, KeyErrorMetric

import pytest


def data_folder(base=None):
    if base is None:
        base = __file__
    base_file = Path(base)
    return base_file.parents[0] / "data"


class TelemetryTests(Exception):
    pass


class FileDoesNotExist(TelemetryTests):
    pass


FileLocation = Union[Path, str]


def process_file_name(file_name: FileLocation) -> Path:
    """
    Shortcut for validating a file and converting it to Path
    """
    file_name = Path(file_name)
    if not file_name.exists():
        raise FileDoesNotExist(f"File {file_name} does not exist")
    return file_name


def load_dump_file(file_name: FileLocation) -> str:
    file_name = process_file_name(file_name)
    with open(file_name, "r") as fh:
        return fh.read()


def load_dump_line(file_name: Union[Path, str], line_number: int):
    """
    Returns a single line of a line. Line start with 1.
    Fails if line does not exist.
    """
    content = load_dump_file(file_name).split("\n")
    if len(content) < line_number:
        raise TelemetryTests(
            f"Line {line_number} is not valid. {file_name} has {len(content)} lines"
        )
    return content[line_number - 1]


# Next are basic tests

BASIC_PROPERTIES = [
    "collection_timestamp",
    "collection_end_time",
    "collection_start_time",
    "msg_timestamp",
    "collection_id",
    "path",
    "node_id",
    "subscription_id",
    "content",
    "data",
]


def check_metric_properties(metric, mandatory):
    """
    Test the basic metric properties.
    No matter the result, it should not raise an exception
    """
    failing_attr = []
    for attr in mandatory:
        try:
            _ = getattr(metric, attr)
        except:
            failing_attr.append(attr)
            raise
    return failing_attr


def check_basic_properties(metric, properties=BASIC_PROPERTIES):
    """
    Test the basic metric properties.
    No matter the result, it should not raise an exception
    """
    failing_attr = []
    for attr in properties:
        try:
            _ = getattr(metric, attr)
        except AttrNotFound as e:
            pass
        except KeyErrorMetric as e:
            pass
        except:
            failing_attr.append(attr)
    return failing_attr


class AbstractTestMetric:
    BASIC_PROPERTIES = [
        "collection_timestamp",
        "collection_end_time",
        "collection_start_time",
        "msg_timestamp",
        "collection_id",
        "path",
        "node_id",
        "subscription_id",
        "content",
        "data",
        "collection_data",
    ]


    @classmethod
    def constructor(cls, value):
        return cls.CLS(value)

    def check_mandatory_properties(self, metric, properties=None):
        """
        Test the basic metric properties.
        No matter the result, it should not raise an exception
        """
        if properties is None:
            properties = self.mandatory_property
        failing_attr = []
        for attr in properties:
            try:
                _ = getattr(metric, attr)
            except:
                failing_attr.append(attr)
        return failing_attr

    def check_basic_properties(self, metric, properties=None):
        """
        Test the basic metric properties.
        No matter the result, it should not raise an exception
        """
        if properties is None:
            properties = self.BASIC_PROPERTIES
        failing_attr = []
        for attr in properties:
            try:
                _ = getattr(metric, attr)
            except AttrNotFound as e:
                pass
            except KeyErrorMetric as e:
                pass
            except:
                failing_attr.append(attr)
        return failing_attr

    def test_mandatory_properties(self, metric):
        failed_attributes = self.check_mandatory_properties(metric)
        assert not failed_attributes

    def test_basic_properties(self, metric):
        failed_attributes = check_basic_properties(metric)
        assert not failed_attributes

    @staticmethod
    def extract_all_except(metric, prop_key):
        return {x: y for x, y in metric.data.items() if x != prop_key}

    @pytest.mark.parametrize(
        "prop, key_prop, new_value_reference",
        [
            ["content", "content_key", {"a": "test"}],
            ["path", "p_key", "new_path"],
            ["keys", "keys_key", {"k": 9}],
            ["collection_data", "collection_data_key", {"k": 9}],
        ],
    )
    def test_replace(self, metric, prop, key_prop, new_value_reference):
        # prop = "content"
        # key_prop = "content_key"
        # new_value_reference = {"a": "test"}
        if prop == "keys" and prop not in self.mandatory_property:
            return

        get_prop = lambda x: getattr(x, prop)
        key = getattr(metric, key_prop)
        new_value = None
        try:
            old_value = get_prop(metric)
            new_value = new_value_reference
        except:
            pass
        if new_value is not None:
            kargs = {prop: new_value}
            new_metric = metric.replace(**kargs)
            assert type(metric) == type(new_metric)
            assert get_prop(new_metric) == new_value
            assert get_prop(metric) != new_value
            assert self.extract_all_except(new_metric, key) == self.extract_all_except(
                metric, key
            )

def pytest_generate_tests(metafunc):
    if issubclass(metafunc.cls, AbstractTestMetric):
        metrics = []
        for metric_value in metafunc.cls.metrics:
            metric = metafunc.cls.constructor(metric_value)
            metrics.append(metric)
        metafunc.parametrize("metric", metrics, scope="class")
