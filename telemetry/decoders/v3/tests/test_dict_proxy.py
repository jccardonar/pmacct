import pytest
from metric_types.dict_proxy import DictProxy, dict_attribute, KeyErrorMetric, AttrNotFound, InvalidClassConstruction


class TestDictAttribute:

    def test_correct_object(self):
        class DummyDictProxy(DictProxy):
            _attr_to_key = {"prop_a": "prop_a_key"}
            prop_a_key = "prop_a_key_in_dict"

            @dict_attribute
            def prop_a(self):
               pass 

        dummy = DummyDictProxy({"prop_a_key_in_dict": 3})
        assert dummy.prop_a == 3

        class SubDummyDictProxy(DummyDictProxy):
            prop_a_key = "prop_a_key_in_dict2"

        dummy = SubDummyDictProxy({"prop_a_key_in_dict2": 4})
        assert dummy.prop_a == 4


    def test_keynot_defined(self):
        with pytest.raises(InvalidClassConstruction):
            class DummyDictProxy(DictProxy):
                _attr_to_key = {"prop_a": "prop_a_key"}

                @dict_attribute
                def prop_a(self):
                   pass 

    def test_attributenot__defined(self):
        with pytest.raises(InvalidClassConstruction):
            class DummyDictProxy(DictProxy):
                _attr_to_key = {"prop_a": "prop_a_key"}
                prop_a_key = "prop_a_key_in_dict"

    def test_prop_without_entry(self):
        with pytest.raises(InvalidClassConstruction):
            class DummyDictProxy(DictProxy):
                _attr_to_key = {}
                prop_a_key = "prop_a_key_in_dict"

                @dict_attribute
                def prop_a(self):
                   pass 

    def test_key_in_attr_does_not_exist(self):
        with pytest.raises(InvalidClassConstruction):
            class DummyDictProxy(DictProxy):
                _attr_to_key = {"prop_aa": "prop_a_key"}
                prop_a_key = "prop_a_key_in_dict"

                @dict_attribute
                def prop_a(self):
                   pass 
