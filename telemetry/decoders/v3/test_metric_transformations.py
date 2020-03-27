import pytest
import json
from encoders.base import InternalMetric
from transformations import transformation_factory, MetricExceptionBase, load_transformtions_from_file
from pprint import pprint

FILE_TESTS = "data_processing_metrics.json"


def load_tests():
    with open(FILE_TESTS) as fh:
        test_description = json.load(fh)
    tests = {}
    for n, test in enumerate(test_description["tests"]):
        name = test.get("name", "noname")
        config = test["config"]
        data = test["data"]
        expected = test["expected"]
        tests[(n, name)] = [config, data, expected]
    return tests


IGNORED_KEYS = {"expected_warning", "exception"}


TESTS = load_tests()


def sort_data(data_list):
    return sorted(
        data_list, key=lambda x: (x["encodingPath"], list(x["keys"].values()))
    )


class TestEncodingTransformation:

    # I dont want data and expected to be in the name since they are long
    @pytest.mark.parametrize("n, name", TESTS.keys())
    def test_transformations(self, capsys, n, name):
        config, data, expected = TESTS[(n, name)]
        sorted_expected = sort_data(expected)
        metric = InternalMetric(data)

        # process operations
        results = []

        transformation = None
        for key in config:
            this_transformation = transformation_factory(key, config)
            if this_transformation is None:
                if key not in IGNORED_KEYS:
                    raise Exception("Option {key} is not recognized")
                continue
            if transformation is not None:
                raise Exception(
                    "We do not support two transformations in the same test"
                )
            transformation = this_transformation

        if transformation is None:
            raise Exception("No transformation found in test")

        def print_exception(warning):
            print(warning)

        transformation.set_warning_function(print_exception)

        if transformation is None:
            pytest.fail("Found no transformation")

        if "exception" in config:
            with pytest.raises(MetricExceptionBase) as excinfo:
                results = list(transformation.transform(metric))
            assert config["exception"] in str(excinfo.value)
            return

        results = list(transformation.transform(metric))

        captured = capsys.readouterr()
        if "expected_warning" in config:
            for text in config["expected_warning"]:
                assert text in captured.out
        else:
            assert not captured.out

        gotten_data = [x.data for x in results]
        sorted_gottan_data = sort_data(gotten_data)
        assert sorted_expected == sorted_gottan_data

    def test_load_transfomations_from_file(self):
        transformations = load_transformtions_from_file("data_test_load_transformations.json")
        assert len(transformations) == 2


