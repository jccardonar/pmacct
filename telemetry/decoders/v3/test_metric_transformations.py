import pytest
import json
from .encoders.base import InternalMetric, ExtraKeysTransformation, CombineTransformationSeries, MetricTransformDummy, RenameKeys
from pygtrie import CharTrie
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

def get_trie(config, key):
    extra_keys_trie = CharTrie()
    extra_keys_trie.update({x: True for x in config[key]})
    return extra_keys_trie

TRIE_KEYS = {"extra_keys"}
def get_operations(config):
    operations = {}
    for key in config:
        if "extra_keys" in config:
            operations[key] = get_trie(config, key)
        if "combine_series" in config:
            operations[key] = config[key]
        if "dummy" in config:
            operations[key] = config[key]
        if "rename_keys" in config:
            operations[key] = config[key]
    leftovers = set(config) - set(operations)
    if leftovers:
        raise Exception(f"We have left keys in config: {leftovers}")
    return operations

TESTS = load_tests()

def sort_data(data_list):
    return sorted(data_list, key=lambda x: (x["encodingPath"], list(x["keys"].values())))

class TestEncodingTransformation:

    # I dont want data and expected to be in the name since they are long
    @pytest.mark.parametrize("n, name", TESTS.keys())
    def test_transformations(self, capsys, n, name):
        config, data, expected = TESTS[(n, name)]
        sorted_expected = sort_data(expected)
        metric = InternalMetric(data)
        operations = get_operations(config)
        
        # process operations
        results = []
        transformation = None
        for operation in operations:
            if "extra_keys" in operation:
                #results.extend(metric.get_extra_keys(operations[operation]))
                transformation = ExtraKeysTransformation(operations[operation])
            if "combine_series" in operation:
                transformations = []
                for key in operations[operation]:
                    if "extra_keys" in key:
                        #results.extend(metric.get_extra_keys(operations[operation]))
                        trie = get_trie(operations[operation], key)
                        transformation = ExtraKeysTransformation(trie)
                        transformations.append(transformation)
                transformation = CombineTransformationSeries(transformations)
            if "dummy" in operation:
                transformation = MetricTransformDummy(None)
            if "rename_keys" in operation:
                transformation = RenameKeys(operations[operation])


        def print_exception(warning):
            print(warning)
        transformation.set_warning_function(print_exception)

        if transformation is None:
            pytest.fail("Found no transformation")

        results = list(transformation.transform(metric))
        if "expected_warning" in config:
            captured = capsys.readouterr()
            for text in config["expected_warning"]:
                assert text in captured.out


        gotten_data = [x.data for x in results]
        sorted_gottan_data = sort_data(gotten_data)
        assert sorted_expected == sorted_gottan_data




