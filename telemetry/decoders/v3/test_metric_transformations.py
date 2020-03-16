import pytest
import json
from encoders.base import (
    InternalMetric,
    ExtraKeysTransformation,
    CombineTransformationSeries,
    MetricTransformDummy,
    RenameKeys,
    MetricExceptionBase,
    SplitLists,
    FieldToString,
    RenameContent,
    CombineContentTransformation,
    FlattenHierarchies,
    TransformationPipeline,
    FilterMetric,
    TransformationPerEncodingPath,
    FlattenHeaders,
)
from pygtrie import CharTrie
from pprint import pprint
from functools import lru_cache

FILE_TESTS = "data_processing_metrics.json"


# TODO: First try to use caching in a trie, since this domites
# some tests. More work is needed
# key here was to cache __contains__
class CacheCharTrie(CharTrie):
    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        self.cache_node = {}
        self.cache = {}
        self.cache_item = {}
        self.complain = False

    def _get_node(self, key, *args, **kargs):
        if key in self.cache:
            return self.cache[key]
        value = super()._get_node(key, *args, **kargs)
        self.cache[key] = value
        return value

    def has_node(self, key):
        if key in self.cache_node:
            return self.cache_node[key]
        value = super().has_node(key)
        self.cache_node[key] = value
        return value

    def __contains__(self, key):
        if key in self.cache_item:
            return self.cache_item[key]
        value = super().__contains__(key)
        self.cache_item[key] = value
        return value


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
    paths = config[key]
    if isinstance(paths, list):
        extra_keys_trie = CacheCharTrie()
        extra_keys_trie.update({x: True for x in paths})
        return extra_keys_trie
    extra_keys_trie = CacheCharTrie()
    extra_keys_trie.update(paths)
    return extra_keys_trie


TRIE_KEYS = {"extra_keys"}
IGNORED_KEYS = {"expected_warning", "exception"}


def get_operations(config):
    operations = {}
    for key in config:
        if "extra_keys" in key:
            operations[key] = get_trie(config, key)
        if "split_lists" in key:
            operations[key] = get_trie(config, key)
        if "combine_series" in key:
            operations[key] = config[key]
        if "combine_content" in key:
            operations[key] = config[key]
        if "dummy" in key:
            operations[key] = config[key]
        if "rename_keys" in key:
            operations[key] = config[key]
        if "field_to_str" in key:
            operations[key] = config[key]
        if "rename_content" in key:
            operations[key] = config[key]
        if "filter" in key:
            operations[key] = config[key]
    leftovers = set(config) - set(operations) - IGNORED_KEYS
    if leftovers:
        raise Exception(f"We have left keys in config: {leftovers}")
    return operations


def transformation_factory(key, data):
    transformation = None
    if "extra_keys" in key:
        paths = get_trie(data, key)
        transformation = ExtraKeysTransformation(paths)
    if "split_lists" in key:
        paths = get_trie(data, key)
        transformation = SplitLists(paths)
    if "dummy" in key:
        transformation = MetricTransformDummy(None)
    if "rename_keys" in key:
        transformation = RenameKeys(data[key])
    if "field_to_str" in key:
        paths = get_trie(data[key], "paths")
        transformation = FieldToString(data[key]["options"], paths)
    if "rename_content" in key:
        paths = get_trie(data, key)
        transformation = RenameContent(paths)
    if "filter" in key:
        transformation = FilterMetric(data[key])
    if "flattening_content" in key:
        if "paths" in data[key]:
            paths = get_trie(data[key], "paths")
            data[key]["paths"] = paths
        transformation = FlattenHierarchies(**data[key])
    if "flattening_headers" in key:
        transformation = FlattenHeaders(**data[key])
    if "trasnformation_per_path" in key:
        config = data[key]
        transformations = CharTrie()
        for path in config:
            for skey in config[path]:
                stransformation = transformation_factory(skey, config)
                if stransformation is None:
                    raise Exception(f"Ilelgal key {skey} in combine_series")
                transformations[path] = stransformation
        transformation = TransformationPerEncodingPath(transformations)
    if "combine_series" in key:
        config = data[key]
        transformations = []
        for skey in config:
            stransformation = transformation_factory(skey, config)
            if stransformation is None:
                raise Exception(f"Ilelgal key {skey} in combine_series")
            transformations.append(stransformation)
        transformation = CombineTransformationSeries(transformations)
    if "combine_content" in key:
        config = data[key]
        transformations = []
        for skey in config:
            stransformation = transformation_factory(skey, config)
            if stransformation is None:
                raise Exception(f"Ilelgal key {skey} in combine_series")
            transformations.append(stransformation)
        transformation = CombineContentTransformation(transformations)
    if "pipeline" in key:
        config = data[key]
        transformations = []
        for skey in config:
            stransformation = transformation_factory(skey, config)
            if stransformation is None:
                raise Exception(f"Ilelgal key {skey} in combine_series")
            transformations.append(stransformation)
        transformation = TransformationPipeline(transformations)
    return transformation


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
        # operations = get_operations(config)
        # transformation = None
        # for operation in operations:
        #    if "extra_keys" in operation:
        #        # results.extend(metric.get_extra_keys(operations[operation]))
        #        transformation = ExtraKeysTransformation(operations[operation])
        #    if "combine_series" in operation:
        #        transformations = []
        #        for key in operations[operation]:
        #            if "extra_keys" in key:
        #                # results.extend(metric.get_extra_keys(operations[operation]))
        #                trie = get_trie(operations[operation], key)
        #                transformation = ExtraKeysTransformation(trie)
        #                transformations.append(transformation)
        #            if "split_lists" in key:
        #                trie = get_trie(operations[operation], key)
        #                transformation = SplitLists(trie)
        #                transformations.append(transformation)
        #        transformation = CombineTransformationSeries(transformations)
        #    if "combine_content" in operation:
        #        transformations = []
        #        for key in operations[operation]:
        #            if "field_to_str" in key:
        #                # results.extend(metric.get_extra_keys(operations[operation]))
        #                paths = get_trie(operations[operation][key], "paths")
        #                transformation = FieldToString(
        #                    operations[operation][key]["options"], paths
        #                )
        #                transformations.append(transformation)
        #            if "rename_content" in key:
        #                paths = get_trie(operations[operation], key)
        #                transformation = RenameContent(paths)
        #                transformations.append(transformation)
        #        transformation = CombineContentTransformation(transformations)
        #    if "dummy" in operation:
        #        transformation = MetricTransformDummy(None)
        #    if "rename_keys" in operation:
        #        transformation = RenameKeys(operations[operation])
        #    if "split_lists" in operation:
        #        transformation = SplitLists(operations[operation])
        #    if "field_to_str" in operation:
        #        paths = get_trie(operations[operation], "paths")
        #        transformation = FieldToString(operations[operation]["options"], paths)
        #    if "rename_content" in operation:
        #        paths = get_trie(operations, operation)
        #        transformation = RenameContent(paths)

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
