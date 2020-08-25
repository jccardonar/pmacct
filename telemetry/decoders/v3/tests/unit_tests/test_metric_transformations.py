import pytest
import json
from transformations.transformations import transformation_factory, MetricExceptionBase, load_transformtions_from_file
from transformations.base_transformation import load_transformation, dump_transformation
from metric_types.base_types import DictElementData

from pprint import pprint
import copy

#FILE_TESTS = "../data_processing_metrics.json"
FILE_TESTS = "data/data_processing_metrics_new.json"


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
        data_list, key=lambda x: (x["encoding_path"], list(x["keys"].values()))
    )

OLD_SYSTEM = False

class TestEncodingTransformation:

    # I dont want data and expected to be in the name since they are long
    #def test_transformations(self, capsys, n, name):
    @pytest.mark.parametrize("n, name", TESTS.keys())
    def test_transformations(self, n, name):
        config, data, expected = TESTS[(n, name)]
        sorted_expected = sort_data(expected)
        original_data = copy.deepcopy(data)
        metric = DictElementData(data)

        # process operations
        results = []

        transformation = None

        exception = None
        expected_warning = []

        if OLD_SYSTEM: 
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
            exception = config.get("exception", None) 
            expected_warning = config.get("expected_warning", [])

        else:
            transformation = load_transformation(config["transformation_config"])
            exception = config.get("exception", None)
            expected_warning = config.get("expected_warning", None)

        if transformation is None:
            raise Exception("No transformation found in test")

        if transformation is None:
            pytest.fail("Found no transformation")

        warnings = []

        if exception is not None:
            with pytest.raises(MetricExceptionBase) as excinfo:
                results = list(transformation.transform(metric))
            assert exception in str(excinfo.value)
            return

        results = list(transformation.transform(metric, warnings))

        #captured = capsys.readouterr()
        if expected_warning:
            for text in expected_warning:
                assert text in ".".join(str(x) for x in warnings)
        else:
            assert not warnings

        # make sure it works without warnings
        results2 = list(transformation.transform(metric))
        assert results == results2

        gotten_data = [x.data for x in results]
        sorted_gottan_data = sort_data(gotten_data)
        assert sorted_expected == sorted_gottan_data

        #now, the original data MUST remain the same
        assert data == original_data

    def test_load_transfomations_from_file(self):
        transformations = load_transformtions_from_file("data/data_test_load_transformations.json")
        assert len(transformations) == 2


