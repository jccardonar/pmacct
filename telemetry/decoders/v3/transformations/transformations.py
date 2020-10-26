from abc import abstractmethod
from enum import Flag, auto
from typing import Any, Dict, Optional, Sequence, Union, Iterable
from cache_char_trie import (
    get_trie,
    get_trie_from_sequence,
    get_trie_from_dict,
    CacheCharTrie,
)

import ujson as json

from .base_transformation import (
    TransformationBase,
    MetricTransformationBase,
    TransformationException,
    load_transformation,
    dump_transformation,
    MetricExceptionBase,
    SubTreeData,
    Field,
)


RANGE_NUMERS = [str(x) for x in range(0, 100)]
HIERARCHICAL_TYPES = (dict, list)


class OptionsFields(Flag):
    """
    Used to identify multiple types where transformation 
    should be applied.
    """

    NO_OPTION = 0
    LISTS = auto()
    FIELDS = auto()  # a field is another field level.
    HIERARCHIES = LISTS | FIELDS

    def to_list(self) -> Sequence[str]:
        """
        Returns a list representing the enum. This actually covers a more complex case with not overalapping 

        >>> OptionsFields.LISTS.to_list()
        ['LISTS']
        >>> (OptionsFields.LISTS | OptionsFields.FIELDS).to_list()
        ['HIERARCHIES']
        """
        if self.name:
            return [self.name]
        # I do this crazy method since decompose is tno a public function.
        return str(self).split(".")[1].split("|")


class GetValuesForContentMixin:
    PATHS_KEY: str = "path_list"
    OPTIONS_KEY: str = "options"
    OPTIONS_LISTS: Sequence["str"] = ["leaf_names"]
    # OTHERS_PARAMS maps properties to values in the dict
    OTHERS_PARAMS: Sequence["str"] = []

    @classmethod
    def from_dict(cls, config):
        # Wel'll make a copy of the dict and modify it to fit the parameters
        class_config = config.copy()
        path_list = class_config.pop(cls.PATHS_KEY, {})
        paths = get_trie_from_sequence(path_list)
        return cls(paths=paths, **class_config)

    def to_dict(self):
        output_dict = {}
        if self.data_per_path:
            output_dict[self.PATHS_KEY] = list(self.data_per_path)
        # Options requires special constructor, since it is a list and we store internally as an enum. Also, we have to store it as text.
        output_dict["options"] = self.options.to_list()

        for prop in self.OPTIONS_LISTS:
            if prop in output_dict:
                continue
            prop_value = getattr(self, prop, None)
            if prop_value:
                output_dict[prop] = list(prop_value)

        for prop in self.OTHERS_PARAMS:
            if prop in output_dict:
                continue
            prop_value = getattr(self, prop, None)
            if prop_value:
                output_dict[prop] = prop_value
        return output_dict


class GetSequenceOfTransformsMixin:
    TRANSFORMATIONS_KEY = "transformations"

    @classmethod
    def from_dict(cls, config):
        trasnformations = []
        for tranformation_config in config.get(cls.TRANSFORMATIONS_KEY, []):
            transformation = load_transformation(tranformation_config)
            trasnformations.append(transformation)

        return cls(trasnformations)

    def to_dict(self):
        tranformation_configs = []
        for transformation in self.transformations:
            transformation_config = dump_transformation(transformation)
            tranformation_configs.append(transformation_config)
        return {self.TRANSFORMATIONS_KEY: tranformation_configs}


class GetTrieFromListMixin:
    PATHS_KEY = "path_list"

    @classmethod
    def from_dict(cls, config):
        paths = get_trie_from_sequence(config[cls.PATHS_KEY])
        return cls(paths)

    def to_dict(self):
        return {self.PATHS_KEY: list(self.data_per_path)}


class GetTrieFromDictMixin:
    PATHS_KEY = "path_info"

    @classmethod
    def from_dict(cls, config):
        paths = get_trie_from_dict(config[cls.PATHS_KEY])
        return cls(paths)

    def to_dict(self):
        return {self.PATHS_KEY: dict(self.data_per_path)}


class SimpleConstructorMixin:
    @classmethod
    def from_dict(cls, config):
        return cls()

    def to_dict(self):
        return {}


class GetDictMixin:
    """
    Similar to GetTrieFromDictMixin but without dumping the content in a trie.
    """

    PATHS_KEY = "path_info"

    @classmethod
    def from_dict(cls, config):
        paths = dict(config[cls.PATHS_KEY])
        return cls(paths)

    def to_dict(self):
        return {self.PATHS_KEY: dict(self.data_per_path)}


class GetListMixin:
    """
    Similar to GetTrieFromDictMixin but without dumping the content in a trie.
    """

    PATHS_KEY = "path_list"

    @classmethod
    def from_dict(cls, config):
        paths = list(config[cls.PATHS_KEY])
        return cls(paths)

    def to_dict(self):
        return {self.PATHS_KEY: list(self.data_per_path)}


def transformation_factory(key, data):
    transformation = None
    if "extra_keys" in key:
        paths = get_trie(data, key)
        transformation = ExtraKeysTransformation(paths)
    elif "split_lists" in key:
        paths = get_trie(data, key)
        transformation = SplitLists(paths)
    elif "dummy" in key:
        transformation = MetricTransformDummy(None)
    elif "rename_keys" in key:
        transformation = RenameKeys(data[key])
    elif "field_to_str" in key:
        paths = get_trie(data[key], "paths")
        transformation = FieldToString(data[key]["options"], paths)
    elif "rename_content" in key:
        paths = get_trie(data, key)
        transformation = RenameContent(paths)
    elif "filter" in key:
        transformation = FilterMetric(data[key])
    elif "flattening_content" in key:
        if "paths" in data[key]:
            paths = get_trie(data[key], "paths")
            data[key]["paths"] = paths
        transformation = FlattenHierarchies(**data[key])
    elif "flattening_headers" in key:
        transformation = FlattenHeaders(**data[key])
    elif "trasnformation_per_path" in key:
        config = data[key]
        transformations = {}
        default = None
        for path in config:
            for skey in config[path]:
                stransformation = transformation_factory(skey, config[path])
                if stransformation is None:
                    raise Exception(f"Ilelgal key {skey} in combine_series")
            if path == "default":
                default = stransformation
            else:
                transformations[path] = stransformation
        transformation = TransformationPerEncodingPath(transformations, default)
    elif "combine_series" in key:
        config = data[key]
        transformations = []
        for skey in config:
            stransformation = transformation_factory(skey, config)
            if stransformation is None:
                raise Exception(f"Ilelgal key {skey} in combine_series")
            transformations.append(stransformation)
        transformation = CombineTransformationSeries(transformations)
    elif "combine_content" in key:
        config = data[key]
        transformations = []
        for skey in config:
            stransformation = transformation_factory(skey, config)
            if stransformation is None:
                raise Exception(f"Ilelgal key {skey} in combine_series")
            transformations.append(stransformation)
        transformation = CombineContentTransformation(transformations)
    elif "pipeline" in key:
        config = data[key]
        transformations = []
        for skey in config:
            stransformation = transformation_factory(skey, config)
            if stransformation is None:
                raise Exception(f"Ilelgal key {skey} in combine_series")
            transformations.append(stransformation)
        transformation = TransformationPipeline(transformations)
    return transformation


def load_transformtions_from_file(json_file):
    """
    Loads a single transformation per file. If more than one is defined, we just take the first
    """
    with open(json_file) as fh:
        objects = json.load(fh)
    transformations = []
    for key in objects:
        this_transformation = transformation_factory(key, objects)
        transformations.append(this_transformation)
    return transformations


class TransformationPerEncodingPath(MetricTransformationBase):
    """
    Applies a transformation per encoding path. Used to quickly filter paths.
    """

    PATHS_KEY = "transformation_per_path"
    DEFAULT_KEY = "default"

    def __init__(self, transformation_per_path, default):
        self.transformation_per_path = transformation_per_path
        self.default = default
        super().__init__(None)

    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        # here, it would be possible to have multiple cases. But that is not the idea.
        transformation = self.transformation_per_path.get(metric.path, self.default)
        if transformation:
            yield from transformation.transform(metric, warnings)
        else:
            yield metric

    @classmethod
    def from_dict(cls, config):
        # all transformations must support the from_dict construction.
        transformation_per_path = {}
        default_transformation = load_transformation(config[cls.DEFAULT_KEY])
        for path, tranformation_config in config.get(cls.PATHS_KEY, {}).items():
            transformation = load_transformation(tranformation_config)
            transformation_per_path[path] = transformation

        return cls(transformation_per_path, default_transformation)

    def to_dict(self):
        output_dict = {self.DEFAULT_KEY: dump_transformation(self.default)}
        transformation_per_path = {}
        for path, transformation in self.transformation_per_path.items():
            transformation_config = dump_transformation(transformation)
            transformation_per_path[path] = transformation_config
        output_dict[self.PATHS_KEY] = transformation_per_path
        return output_dict


class EqualTransformation(SimpleConstructorMixin, MetricTransformationBase):
    """
    Used for cases where we apply a transformation but we
    just want the same metric
    """

    def __init__(self):
        super().__init__({})

    def transform(self, metric, warnings=None):
        yield metric


# A global is fine to avoid extra constructors
EQUAL_TRANSFORMATION = EqualTransformation()


class FilterMetric(GetListMixin, MetricTransformationBase):
    """
    We just filter by encoding path. Other more complex filters can be done,
    but we dont need the now.
    """

    def transform(self, metric, warnings=None):
        if metric.path in self.data_per_path:
            return
        yield metric


# what to select:
# by leaf name, by leaf substring, by type of node (leaf, hiearchy, list, field)
# by encoding path


class ContentTransformation(MetricTransformationBase):
    """
    General classes that transform content (do not yield multiple,metrics, only one transformed)
    It first transforms the hierachically fields, in order, for instance, to flatten internal hierarchies first and then the current
    """

    def __init__(self, options, paths, leaf_names=None):
        if leaf_names is None:
            leaf_names = set()
        self.leaf_names = leaf_names
        self.options = OptionsFields.NO_OPTION
        self.transform_list_elements = True
        for option_str in options:
            option = getattr(OptionsFields, option_str)
            self.options = self.options | option
        super().__init__(paths)

    def has_node(self, path) -> bool:
        """
        Returns true if the transformation covers a node.
        """
        # if individual nammes are mamtched, then continue
        if self.leaf_names:
            return True
        if self.options.value > 0:
            # we have options, we need to look at everything.
            return True
        return self.data_per_path.has_node(path) > 0

    def has_key_path(self, path, key, value) -> bool:
        """
        Returns true if the transformation covers a key.
        """
        if self.leaf_names and key in self.leaf_names:
            return (True, path)
        if self.options.value > 0:
            if isinstance(value, list) and OptionsFields.LISTS in self.options:
                return (True, path)
            if isinstance(value, dict) and OptionsFields.FIELDS in self.options:
                return (True, path)
        if path in self.data_per_path:
            return (True, path)
        return (False, path)

    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        fields = self._transform_contents(metric, metric.content, metric.path, warnings)
        yield metric.replace(content=fields)

    def _transform_contents(
        self, metric, fields: Union[Sequence[Field], Field], path: str, warnings
    ):
        """
        This function is a generator that also returns values.
        The return includes the new set of fields, and a bool marking whether
        the fields changed from the input.
        """

        # if the path is not included just return
        if not self.has_node(path):
            return fields

        # if we are in a list, we check one by one the elements and create a new list with
        # the results
        if isinstance(fields, list):
            if not self.transform_list_elements:
                return fields
            nlist = []
            for field in fields:
                if not isinstance(field, HIERARCHICAL_TYPES):
                    nlist.append(field)
                    continue
                ncontents = self._transform_contents(metric, field, path, warnings)
                if ncontents:
                    nlist.append(ncontents)
            # nothing else to do
            return nlist

        # from here, fields is a "field"
        new_keys = {}
        key_state = {}
        fields_with_children = {}
        fields = fields.copy()

        # we go over the fields, checking whether we have a new key.
        # We add them all at the end
        for fname, fcontent in fields.items():
            n_path = metric.form_encoding_path(path, [fname])
            has_key_path, state = self.has_key_path(n_path, fname, fcontent)
            if has_key_path:
                # we are in a new key here.
                # we dont even check for type of content.
                # Although that could break stuff.
                new_keys[fname] = fcontent
                key_state[fname] = state

            if isinstance(fcontent, HIERARCHICAL_TYPES):
                fields_with_children[fname] = n_path

        # first convert the children
        if fields_with_children:
            # this means we have fields with children:
            # lists or composed fields.
            for fname, n_path in fields_with_children.items():
                fcontent = fields[fname]
                # if there is no content we ignore.
                if not fcontent:
                    continue
                # we yield all internal keys, then we replace
                # the value if there was any change.
                ncontents = self._transform_contents(metric, fcontent, n_path, warnings)
                fields.pop(fname, None)
                # not write the new field if empty
                if ncontents:
                    fields[fname] = ncontents

        # now convert the fields, only if there are matches.
        if new_keys:
            fields = self.transform_content(
                metric, fields, path, new_keys, key_state, warnings
            )
        return fields

    @abstractmethod
    def transform_content(self, metric, fields, path, new_keys, key_state, warnings):
        pass


class FieldTransformation(GetValuesForContentMixin, ContentTransformation):
    """
    A transformation where the field transformtion is simple and done
    in a separate function.
    if you return None, the key will be removed
    """

    @abstractmethod
    def field_transformation(self, key, value):
        pass

    def transform_content(self, metric, fields, path, new_keys, key_state, warnings):
        new_fields = fields.copy()
        for key in new_keys:
            value = fields[key]
            new_value = self.field_transformation(key, value)
            if new_value is None:
                new_fields.pop(key, None)
                continue
            new_fields[key] = new_value
        return new_fields


class ValueMapper(FieldTransformation):
    """
    Maps values. Good for transforming enums from ints to strings, strings to strings, or viceversa.
    """

    OTHERS_PARAMS = FieldTransformation.OTHERS_PARAMS
    OTHERS_PARAMS.append("mapper")
    OTHERS_PARAMS.append("default")

    def field_transformation(self, key, value):
        return self.mapper.get(value, self.default)

    def __init__(self, mapper: Dict[Any, Any], default: Optional[Any], *args, **kargs):
        if not mapper or not isinstance(mapper, dict):
            raise TransformationException("ValueMapper requires a mapper and a default")
        self.mapper = mapper
        self.default = default
        super().__init__(*args, **kargs)


class ConvertToList(FieldTransformation):
    """
    Forces a container to be a list.
    We enclosure the value into a list.
    """

    def field_transformation(self, key, value):
        if not isinstance(value, list):
            return [value]
        return value


class ConvertToint(FieldTransformation):
    def field_transformation(self, key, value):
        # we fail here if we cannot convert
        try:
            new_value = int(value)
        except:
            raise TransformationException("We could not convert value to int")
        return new_value


class WrapContent:
    """
    This is a lazy version of something that would "move up" the encoding path.
    The encoding path would need to be changed in other transformtion.
    """

    def __init__(self, container):
        super().__init__(None)
        self.containers = container

    def transform(self, metric):
        yield metric.replace(content={self.containers: metric.content})


class FieldToString(GetValuesForContentMixin, ContentTransformation):
    @staticmethod
    def string_field(value):
        if isinstance(value, str):
            new_value = value
            return new_value
        return json.dumps(value)

    def transform_content(self, metric, fields, path, new_keys, key_state, warnings):
        new_fields = fields.copy()
        for key in new_keys:
            value = fields[key]

            new_value = self.string_field(value)

            new_fields[key] = new_value
        return new_fields


class FlattenningLists(MetricExceptionBase):
    pass


class ExistingNameInFlattening(MetricExceptionBase):
    pass


class FlattenFunctions:
    """
    Flatten functions. Choosing inheritance over composition here.
    """

    def __init__(self, keep_naming):
        self.keep_naming = keep_naming

    def find_name(self, fields, key, ckey, path, warnings) -> str:
        prefix = ""
        if self.keep_naming:
            prefix = key
        if prefix:
            base_name = "_".join([prefix, ckey])
        else:
            base_name = ckey
        name = None
        if base_name in fields:
            warnings.append(
                ExistingNameInFlattening(
                    "Base name for flattening exists",
                    {"base": base_name, "path": path, "child_key": ckey},
                )
            )
            for x in RANGE_NUMERS:
                candidate = "_".join([base_name, str(x)])
                if candidate not in fields:
                    break
            else:
                raise ExistingNameInFlattening(
                    "Cound not Find a name for hierarchy",
                    {"path": path, "child_key": ckey},
                )
            # find a name with the structure prefix_name_number, but complaint.
            name = candidate
        else:
            name = base_name

        return name


class TransformationPipeline(GetSequenceOfTransformsMixin, MetricTransformationBase):
    def __init__(self, transformations: Sequence[TransformationBase]):
        self.transformations = transformations
        super().__init__(None)

    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        gen = iter([metric])
        for trf in self.transformations:
            gen = trf.transform_list(gen, warnings)
        yield from gen


class KeysFlattenOverlap(MetricExceptionBase):
    pass


class KeysWithDoubleName(MetricExceptionBase):
    pass


class FlattenHeaders(
    SimpleConstructorMixin, MetricTransformationBase, FlattenFunctions
):
    """
    Flattens metadata into content of the metric.
    Design decisions:
    """

    def __init__(self):
        self.keep_naming = False
        super().__init__(None)

    def add_to_field(self, fields, key, value, path, warnings):
        nname = self.find_name(fields, "keys", key, path, warnings)
        if nname in fields:
            raise ExistingNameInFlattening(
                "Find name returned an existing value",
                {"name": nname, "path": path, "child_key": key},
            )
        fields[nname] = value

    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        new_fields = {}
        self.flatten_keys(metric.keys, metric.path, new_fields, warnings)
        self.add_to_field(
            new_fields, metric.msg_timestamp_key, metric.msg_timestamp, metric.path, warnings
        )
        self.add_to_field(
            new_fields, metric.node_key, metric.node_id, metric.path, warnings
        )

        # now the content
        for key, value in metric.content.items():
            self.add_to_field(new_fields, key, value, metric.path, warnings)

        yield metric.replace(content=new_fields)

    def flatten_keys(self, keys, path, new_fields, warnings):
        for key, value in keys.items():
            if isinstance(value, list):
                for svalue in value:
                    self.add_to_field(new_fields, key, svalue, path, warnings)
                warnings.append(
                    KeysWithDoubleName(
                        "Keys with the same name", {"name": key, "path": path}
                    )
                )
                continue
            self.add_to_field(new_fields, key, value, path, warnings)


class InvalidFlatteningPath(MetricExceptionBase):
    pass


class FlattenHierarchies(
    GetValuesForContentMixin, ContentTransformation, FlattenFunctions
):
    OPTIONS_LISTS = []
    OTHERS_PARAMS = ["keep_naming"]

    def __init__(self, keep_naming=False, paths=None, options=None):
        if options is None:
            # if there are paths, keep empty, if there are not, set all
            options = []
            if paths is None:
                options = ["HIERARCHIES"]
        if paths is None:
            paths = CacheCharTrie()
        super().__init__(options, paths)
        self.keep_naming = keep_naming
        self.transform_list_elements = True

    def transform_content(self, metric, fields, path, new_keys, key_state, warnings):
        new_fields = fields.copy()
        # the sorted makes this a bit more deterministic
        for key in sorted(new_keys):
            # all keys are the hierarchical ones.
            value = new_fields.pop(key)
            kpath = key_state[key]

            if not isinstance(value, HIERARCHICAL_TYPES):
                warnings.append(
                    InvalidFlatteningPath(
                        "Ignoring flattening path, it is not a list or a field",
                        {"path": kpath},
                    )
                )
                # placing the value back, this should not be the norm.
                new_fields[key] = value
                continue

            #           # deal with lists
            if isinstance(value, list):
                warnings.append(FlattenningLists("Flattening lists", {"path": kpath}))
                nvalue = FieldToString.string_field(value)
                new_fields[key] = nvalue
                continue

            # this must be a dict, that should be already flatten
            for ckey, cvalue in value.items():
                nname = self.find_name(new_fields, key, ckey, kpath, warnings)
                if nname in new_fields:
                    raise ExistingNameInFlattening(
                        "Find name returned an existing value",
                        {"name": nname, "path": kpath, "child_key": ckey},
                    )
                new_fields[nname] = cvalue
        return new_fields


class CombineContentTransformation(GetSequenceOfTransformsMixin, ContentTransformation):
    def __init__(self, transformations: Sequence["ContentTransformation"]):
        super().__init__([], None)
        self.transformations = transformations

    def has_node(self, path: str) -> bool:
        """
        Checks whether an encoding path is covered by the operation
        """
        for t in self.transformations:
            if t.has_node(path):
                return True
        return False

    def has_key_path(self, path, key, value):
        """
        Checks whether the operation applies to a field
        """
        global_state = {}
        global_has_key_path = False
        for n, t in enumerate(self.transformations):
            has, state = t.has_key_path(path, key, value)
            if has:
                global_has_key_path = True
                global_state[n] = state
        return (global_has_key_path, global_state)

    def transform_content(self, metric, fields, path, new_keys, key_state, warnings):
        new_fields = fields.copy()
        # we need to do a conversion here, which is a pity
        state_per_transformer = {}

        for key, n_state in key_state.items():
            for n, state in n_state.items():
                state_per_transformer.setdefault(n, {})[key] = state
        for n in sorted(state_per_transformer):
            state = state_per_transformer[n]
            transformation = self.transformations[n]
            new_fields = transformation.transform_content(
                metric, new_fields, path, state, state, warnings
            )
        return new_fields


class ExsitingName(MetricExceptionBase):
    pass


class RenameContent(GetTrieFromDictMixin, ContentTransformation):
    def __init__(self, paths):
        super().__init__([], paths)

    def transform_content(self, metric, fields, path, new_keys, key_state, warnings):
        new_fields = fields.copy()
        for key in new_keys:
            path = key_state[key]
            new_key = self.data_per_path[path]
            if new_key is None:
                new_fields.pop(key, None)
                continue
            if new_key in new_fields:
                warnings.append(
                    ExsitingName(
                        "Name already exists in path", {"path": path, "name": new_key}
                    )
                )
                continue
            value = new_fields.pop(key)
            new_fields[new_key] = value
        return new_fields


class RKEWrongType(MetricExceptionBase):
    pass

# TODO
# move this to a single part, it is duplicated with cisco_gpbvkv.py
def add_to_flatten(flatten_content, key, value):
    if key in flatten_content:
        current_state = flatten_content[key]
        if isinstance(current_state, list):
            current_state.append(value)
        else:
            current_list = [current_state, value]
            flatten_content[key] = current_list
        return
    flatten_content[key] = value

def add_keys(current_keys, extra_keys):
    new_keys = current_keys.copy()
    for key, value in extra_keys.items():
        add_to_flatten(new_keys, key, value)
    return new_keys

class RenameKeys(GetDictMixin, MetricTransformationBase):
    def transform(self, metric, warnings=None):
        """
        Simply modify the keys.
        If the list contain an index not present in the actual keys, this is ignored.
        self.data_per_path is a dict. When keys are repeated, the value is other dict with indices
        """
        # if the path is not included, just return the same
        if metric.path not in self.data_per_path:
            yield metric
            return
        # get the info for this path
        path_data = self.data_per_path[metric.path]
        current_keys = {}
        # we use as a base the info on the metric key.
        for key, value in metric.keys.items():
            if key not in path_data:
                add_to_flatten(current_keys, key, value)
                continue
            new_key_info = path_data[key]
            if not isinstance(new_key_info, dict):
                new_key = new_key_info
                add_to_flatten(current_keys, new_key, value)
                continue
            # repeated values, value MUST be a list.
            if not isinstance(value, list):
                raise RKEWrongType(
                    "Key value is not a list but modification is a dict",
                    {"key": key, "path": metric.path},
                )
            key_values = value
            for n, value in enumerate(key_values):
                n = str(n)
                if n not in new_key_info:
                    add_to_flatten(current_keys, key, value)
                    continue
                # we have a new name
                new_key = new_key_info[n]
                add_to_flatten(current_keys, new_key, value)
        yield metric.replace(keys=current_keys)


class MetricSpliting(MetricTransformationBase):
    """
    Base functioning of splitting the metric similar to how the extrq keys function. Other splitting operations might have the same characteristics.
    It "splits" the current metric then it repeats the operation with the internal ones (in order for the next to, for instance, have the proper new keys)
    """

    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        fields, changed = yield from self._split(
            metric, metric.content, metric.path, warnings
        )
        if changed:
            if fields:
                yield metric.replace(content=fields)
            return
        yield metric

    def has_node(self, path: str) -> bool:
        """
        Checks whether an encoding path is covered by the operation
        """
        return self.data_per_path.has_node(path) > 0

    def has_key_path(self, path: str) -> bool:
        """
        Checks whether the operation applies to a field, and returns state that is needed later
        """
        return (path in self.data_per_path, path)

    @abstractmethod
    def split(
        self, metric, fields, path, keys, key_state, warnings
    ) -> Sequence[SubTreeData]:
        pass

    def _split(
        self, metric, fields: Union[Sequence[Field], Field], path: str, warnings
    ):
        """
        This function is a generator that also returns values.
        The return includes the new set of fields, and a bool marking whether
        the fields changed from the input.
        """
        changed = False

        # if the path is not included just return
        if not self.has_node(path):
            return fields, changed

        # if we are in a list, we check one by one the elements and create a new list with
        # the results
        if isinstance(fields, list):
            nlist = []
            for field in fields:
                ncontents, cchanged = yield from self._split(
                    metric, field, path, warnings
                )
                if cchanged:
                    changed = True
                if ncontents:
                    nlist.append(ncontents)
            # nothing else to do
            return nlist, changed

        # from here, fields is a "field"
        new_keys = {}
        key_state = {}
        fields_with_children = {}
        fields = fields.copy()

        # we go over the fields, checking whether we have a new key.
        # We add them all at the end
        for fname, fcontent in fields.items():
            n_path = metric.form_encoding_path(path, [fname])
            has_key_path, state = self.has_key_path(n_path)
            if has_key_path:
                # we are in a new key here.
                # we dont even check for type of content.
                # Although that could break stuff.
                new_keys[fname] = fcontent
                key_state[fname] = state

            if isinstance(fcontent, HIERARCHICAL_TYPES):
                fields_with_children[fname] = n_path

        # if we have any new keys, we need to split here. We do it and yield
        # anything new from the new metric (we ignore the fields_with_children
        if new_keys:
            # we need to split the metric here.
            new_content, changed = yield from self.split(
                metric, fields, path, new_keys, key_state, warnings
            )
            # we return empty.
            return new_content, changed

        if fields_with_children:
            # if we have children, this means we have children
            # lists or composed fields.
            for fname, n_path in fields_with_children.items():
                fcontent = fields[fname]
                # if there is no content we ignore.
                if not fcontent:
                    continue
                # we yield all internal splits, then we replace
                # the value if there was any change.
                ncontents, cchanged = yield from self._split(
                    metric, fcontent, n_path, warnings
                )
                if cchanged:
                    changed = True
                    fields.pop(fname, None)
                    # not write the new field if empty
                    if ncontents:
                        fields[fname] = ncontents

        return fields, changed


class ExtraKeysTransformation(GetTrieFromListMixin, MetricSpliting):
    def split(self, metric, fields, path, keys, key_state, warnings):
        new_keys = keys
        current_keys = metric.keys
        new_keys = add_keys(current_keys, new_keys)
        current_content = fields
        for key in new_keys:
            current_content.pop(key, None)
        new_metric = metric.replace(content=current_content, keys=new_keys, path=path)
        # now, we need to apply the change also here
        yield from self.transform(new_metric)
        return {}, True


class NotAList(MetricExceptionBase):
    pass


class SplitLists(GetTrieFromListMixin, MetricSpliting):
    def split(self, metric, fields, path, keys, key_state, warnings):
        new_keys = keys
        current_content = fields
        for key in new_keys:
            kpath = key_state[key]
            elements = current_content.pop(key, None)
            if not isinstance(elements, list):
                warnings.append(
                    NotAList(
                        "Trying to split an element which is not a list", {"key": key}
                    )
                )
                continue
            for element in elements:
                new_metric = metric.replace(content=element, path=kpath)
                yield from self.transform(new_metric)
        return current_content, True


class CombineTransformationSeries(GetSequenceOfTransformsMixin, MetricSpliting):
    """
    This is kind of broken implementation, since it has state. Not sure how to do it better
    without needing to pack and unpack too many times.
    """

    def __init__(self, transformations: Sequence["MetricSplit"]):
        self.transformations = transformations

    def has_node(self, path: str) -> bool:
        """
        Checks whether an encoding path is covered by the operation
        """
        for t in self.transformations:
            if t.has_node(path):
                return True
        return False

    def has_key_path(self, path: str):
        """
        Checks whether the operation applies to a field
        """
        for n, t in enumerate(self.transformations):
            has, state = t.has_key_path(path)
            if has:
                return (True, (n, state))
        return (False, (None, None))

    def split(
        self, metric, fields, path, keys, key_state, warnings
    ) -> Sequence[SubTreeData]:
        # here we apply only the first operation
        state_per_transform = {}
        for key, (n, state) in key_state.items():
            state_per_transform.setdefault(n, {})[key] = state
        min_n = min(state_per_transform)
        state = state_per_transform[min_n]
        keys_content = {x: y for x, y in keys.items() if x in state}
        value = yield from self.transform_list(
            self.transformations[min_n].split(
                metric, fields, path, keys_content, state, warnings
            )
        )
        return value


class MetricWarningDummy(MetricExceptionBase):
    pass


class MetricTransformDummy(SimpleConstructorMixin, MetricTransformationBase):
    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        warnings.append(MetricWarningDummy("Dummy warning", {"df": 2}))
        yield metric


class RemoveContentHierarchies(SimpleConstructorMixin, MetricTransformationBase):
    """
    Forces content to be a dict, it might yield multiple metrics.
        - if it is dict, it remains.
        - if it is a list, it yields a metric per element (and continues recursively)
    """

    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        if isinstance(metric.content, dict):
            yield metric
        elif isinstance(metric.content, list):
            for elem in metric.content:
                new_metric = metric.replace(content=elem)
                yield from self.transform(new_metric)
        warnings.append(
            TransformationException(
                "Found a type different from list or dict in content"
            )
        )
        yield metric


def metric_to_json_dict(metric, content_base=True, content_key="content"):
    """
    Returns a dict from a metric.
    The dict should be converted to json later.
    """
    if not content_base:
        return metric.to_dict()

    content = metric.content.copy()
    if not isinstance(content, dict):
        content = {content_key: content}
    keys = None
    try:
        keys = getattr(metric, "keys", None)
    except:
        pass
    if keys is not None and isinstance(keys, dict):
        content.update(keys)
    content.update(metric.headers)
    return content