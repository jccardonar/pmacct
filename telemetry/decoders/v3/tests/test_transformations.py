import pytest
from transformations.transformations import (
    EqualTransformation,
    ExtraKeysTransformation,
    SplitLists,
    RenameKeys,
    MetricTransformDummy,
    FieldToString,
    RenameContent,
    FilterMetric,
    FlattenHierarchies,
    FlattenHeaders,
    TransformationPerEncodingPath,
    CombineTransformationSeries,
    CombineContentTransformation,
    TransformationPipeline,
    ConvertToList,
    ValueMapper,
    RemoveContentHierarchies,
)
from transformations.base_transformation import (
    load_transformation,
    dump_transformation,
    load_transformations,
    dump_transformations,
)
from transformations import base_transformation
from transformations import transformations

from mock import Mock
ma = Mock(wraps=load_transformation)
# mock the different versions, not sure how to do this better.
load_transformation = ma
base_transformation.load_transformation = ma
transformations.load_transformation = ma


TRANSFORMATION_TESTS = [
    (ExtraKeysTransformation, {"path_list": ["extra_key1", "extra_key2"]}),
    (SplitLists, {"path_list": ["extra_key1", "extra_key2"]}),
    (EqualTransformation, {}),
    (MetricTransformDummy, {}),
    (RenameKeys, {"path_info": {"extra_key1": "extra_key2"}}),
    (
        FieldToString,
        {
            "path_list": ["extra_key1", "extra_key2"],
            "options": ["HIERARCHIES"],
            "leaf_names": ["leaf1", "a"],
        },
    ),
    (RenameContent, {"path_info": {"extra_key1": "extra_key2"}}),
    (FilterMetric, {"path_list": ["extra_key1", "extra_key2"]}),
    (
        FlattenHierarchies,
        {
            "path_list": ["extra_key1", "extra_key2"],
            "options": ["HIERARCHIES"],
            "keep_naming": True,
        },
    ),
    (FlattenHeaders, {}),
    (
        TransformationPerEncodingPath,
        {
            "transformation_per_path": {
                "path1": {"transformation": "EqualTransformation", "config": {}},
                "path2": {"transformation": "MetricTransformDummy", "config": {}},
                "path3": {
                    "transformation": "FlattenHierarchies",
                    "config": {
                        "path_list": ["extra_key1", "extra_key2"],
                        "options": ["HIERARCHIES"],
                        "keep_naming": True,
                    },
                },
            },
            "default": {"transformation": "EqualTransformation", "config": {}},
        },
    ),
    (
        CombineTransformationSeries,
        {
            "transformations": [
                {"transformation": "EqualTransformation", "config": {}},
                {"transformation": "MetricTransformDummy", "config": {}},
                {
                    "transformation": "FlattenHierarchies",
                    "config": {
                        "path_list": ["extra_key1", "extra_key2"],
                        "options": ["HIERARCHIES"],
                        "keep_naming": True,
                    },
                },
            ]
        },
    ),
    (
        CombineContentTransformation,
        {
            "transformations": [
                {"transformation": "EqualTransformation", "config": {}},
                {"transformation": "MetricTransformDummy", "config": {}},
                {
                    "transformation": "FlattenHierarchies",
                    "config": {
                        "path_list": ["extra_key1", "extra_key2"],
                        "options": ["HIERARCHIES"],
                        "keep_naming": True,
                    },
                },
            ]
        },
    ),
    (
        TransformationPipeline,
        {
            "transformations": [
                {"transformation": "EqualTransformation", "config": {}},
                {"transformation": "MetricTransformDummy", "config": {}},
                {
                    "transformation": "FlattenHierarchies",
                    "config": {
                        "path_list": ["extra_key1", "extra_key2"],
                        "options": ["HIERARCHIES"],
                        "keep_naming": True,
                    },
                },
            ]
        },
    ),
    (
        ConvertToList,
        {
            "options": ["LISTS"],
        }
    ),
    (
        ValueMapper,
        {
            "options": ["LISTS"],
            "path_list": ["extra_key1", "extra_key2"],
            "mapper": {"TWO": 2},
            "default": 2,
        }
    ),
    (RemoveContentHierarchies, {}),
]


class TestTransformtionConstruction:
    """
    Test the correct construction of multiple transformations.
    """

    @pytest.mark.parametrize("t_class,config_dict", TRANSFORMATION_TESTS)
    def test_tranformation_construction(self, t_class, config_dict):
        """
        Test the from_dict, to_dict of the functions.
        """
        instance = t_class.from_dict(config_dict)
        assert config_dict == instance.to_dict()

        # this would make sure the transformation has a proper DICT_KEY defined
            
        load_transformation(dump_transformation(instance))

    def test_load_dump_transformation_general(self):
        config = {"transformation": "EqualTransformation", "config": {}}
        assert config == dump_transformation(load_transformation(config))

        config = [
            {"transformation": "EqualTransformation", "config": {}},
            {"transformation": "EqualTransformation", "config": {}},
        ]
        assert config == dump_transformations(load_transformations(config))

    @classmethod
    def teardown_class(cls):
        tested_classes = set([op[0][0]["transformation"] for op in ma.call_args_list])
        all_classes = set(base_transformation.TransformationBase.TRANSFORMATIONS)
        classes_to_ignore = set(["MultiplyMetric", "ContentTransformation", "MetricSpliting", "BaseConverter", "TransformationWarningIf", "TransformationErrorIf", "MetricTransformationBase",]) 
        missing = all_classes - tested_classes - classes_to_ignore
        print("Next transformation are missing tests:", missing)




