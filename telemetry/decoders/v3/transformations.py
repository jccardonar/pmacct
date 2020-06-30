from encoders.base import ValueField, Field, InternalMetric, MetricExceptionBase
from typing import Iterable, Sequence, Dict, Union, Any, Generator, Tuple, Optional
from abc import ABC, abstractmethod
from enum import Flag, auto
import ujson as json
from pygtrie import CharTrie
from base_transformation import MetricTransformationBase, TransformationException

RANGE_NUMERS = [str(x) for x in range(0, 100)]
HIERARCHICAL_TYPES = (dict, list)

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

def get_trie(config, key):
    paths = config[key]
    if isinstance(paths, list):
        extra_keys_trie = CacheCharTrie()
        extra_keys_trie.update({x: True for x in paths})
        return extra_keys_trie
    extra_keys_trie = CacheCharTrie()
    extra_keys_trie.update(paths)
    return extra_keys_trie

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

def load_transformtions_from_file(json_file):
    '''
    Loads a single transformation per file. If more than one is defined, we just take the first
    '''
    with open(json_file) as fh:
        objects = json.load(fh)
    transformations = []
    for key in objects:
        this_transformation = transformation_factory(key, objects)
        transformations.append(this_transformation)
    return transformations



class FailingOnWarning(Exception):
    pass



class TransformationPerEncodingPath(MetricTransformationBase):
    """
    Applies a transformation per encoding path. Used to quickly filter paths.
    """
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

class EqualTransformation(MetricTransformationBase):
    '''
    Used for cases where we apply a transformation but we
    just want the same metric
    '''
    def __init__(self):
        super().__init__({})

    def transform(self, metric):
        yield metric

# A global is fine to avoid extra constructors
EQUAL_TRANSFORMATION = EqualTransformation()

class FilterMetric(MetricTransformationBase):
    """
    We just filter by encoding path. Other more complex filters can be done,
    but we dont need the now.
    """

    def transform(self, metric, warnings=None):
        if metric.path in self.data_per_path:
            return
        yield metric


class OptionsFields(Flag):
    NO_OPTION = 0
    LISTS = auto()
    FIELDS = auto()  # a field is another field level.
    HIERARCHIES = LISTS | FIELDS


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

    def has_node(self, path):
        # if individual nammes are mamtched, then continue
        if self.leaf_names:
            return True
        if self.options.value > 0:
            # we have options, we need to look at everything.
            return True
        return self.data_per_path.has_node(path) > 0

    def has_key(self, path, key, value):
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
            has_key, state = self.has_key(n_path, fname, fcontent)
            if has_key:
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
            fields = self.transform_content(metric, fields, path, new_keys, key_state, warnings)
        return fields

    @abstractmethod
    def transform_content(metric, fields, path, new_keys, key_state, warnings):
        pass

class FieldTransformation(ContentTransformation):
    '''
    Defines the transformatin that should happen.
    if you return None, the key will be removed
    '''

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

class GenericFieldTransformation(FieldTransformation):
    def __init__(self, *args, this_function, **kargs):
        self.field_transformation = this_function
        super().__init__(*args, **kargs)

class ValueMapper(FieldTransformation):
    '''
    Maps values. Good for transforming enums from ints to strings, strings to strings, or viceversa.
    '''
    @abstractmethod
    def field_transformation(self, key, value):
        return self.mapper.get(value, self.default)

    def __init__(self, mapper: Dict[Any, Any], default: Optional[Any], *args, **kargs):
        if not mapper or not isinstance(mapper, dict):
            raise TransformationException("ValueMapper requires a mapper and a default")
        self.mapper = mapper
        self.default = default
        super().__init__(*args, **kargs)

class ConvertToList(FieldTransformation):
    '''
    Forces a container to be a list.
    We enclosure the value into a list.
    '''
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
    '''
    This is a lazy version of something that would "move up" the encoding path.
    The encoding path would need to be changed in other transformtion.
    '''
    def __init__(self, container):
        super().__init__(None)
        self.containers = container

    def transform(self, metric):
        yield metric.replace(content={self.container: metric.content})
        

class FieldToString(ContentTransformation):
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


class TransformationPipeline(MetricTransformationBase):
    def __init__(self, transformations):
        self.transformations = transformations
        super().__init__(None)

    def set_warning_function(self, warning_function):
        self._warning = warning_function
        for trs in self.transformations:
            trs.set_warning_function(warning_function)

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


class FlattenHeaders(MetricTransformationBase, FlattenFunctions):
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
            new_fields, metric.timestamp_key, metric.timestamp, metric.path, warnings
        )
        self.add_to_field(new_fields, metric.node_key, metric.node, metric.path, warnings)

        # now the content
        for key, value in metric.content.items():
            self.add_to_field(new_fields, key, value, metric.path, warnings)

        yield metric.replace(content=new_fields)

    def flatten_keys(self, keys, path, new_fields, warnings):
        for key, value in keys.items():
            if isinstance(value, list):
                for svalue in value:
                    self.add_to_field(new_fields, key, value, path, warnings)
                warnings.append(
                    KeysWithDoubleName(
                        "Keys with the same name", {"name": key, "path": path}
                    )
                )
                continue
            self.add_to_field(new_fields, key, value, path, warnings)


class InvalidFlatteningPath(MetricExceptionBase):
    pass


class FlattenHierarchies(ContentTransformation, FlattenFunctions):
    def __init__(self, keep_naming=False, paths=None, options=None):
        if options == None:
            # if there are paths, keep empty, if there are not, set all
            options = []
            if paths is None:
                options = ["HIERARCHIES"]
        if paths is None:
            paths = CharTrie()
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


class CombineContentTransformation(ContentTransformation):
    def __init__(self, transformations: Sequence["ContentTransformation"]):
        super().__init__([], None)
        self.transformations = transformations

    def set_warning_function(self, warning_function):
        self._warning = warning_function
        for trs in self.transformations:
            trs.set_warning_function(warning_function)

    def has_node(self, path: str) -> bool:
        """
        Checks whether an encoding path is covered by the operation
        """
        for t in self.transformations:
            if t.has_node(path):
                return True
        return False

    def has_key(self, path, key, value):
        """
        Checks whether the operation applies to a field
        """
        global_state = {}
        global_has_key = False
        for n, t in enumerate(self.transformations):
            has, state = t.has_key(path, key, value)
            if has:
                global_has_key = True
                global_state[n] = state
        return (global_has_key, global_state)

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


class RenameContent(ContentTransformation):
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


class RenameKeys(MetricTransformationBase):
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
                metric.add_to_flatten(current_keys, key, value)
                continue
            new_key_info = path_data[key]
            if not isinstance(new_key_info, dict):
                new_key = new_key_info
                metric.add_to_flatten(current_keys, new_key, value)
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
                    metric.add_to_flatten(current_keys, key, value)
                    continue
                # we have a new name
                new_key = new_key_info[n]
                metric.add_to_flatten(current_keys, new_key, value)
        yield metric.replace(keys=current_keys)



class MetricSpliting(MetricTransformationBase):
    """
    Base functioning of splitting the metric similar to how the extrq keys function. Other splitting operations might have the same characteristics.
    It "splits" the current metric then it repeats the operation with the internal ones (in order for the next to, for instance, have the proper new keys)
    """

    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        fields, changed = yield from self._split(metric, metric.content, metric.path, warnings)
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

    def has_key(self, path: str) -> bool:
        """
        Checks whether the operation applies to a field, and returns state that is needed later
        """
        return (path in self.data_per_path, path)

    @abstractmethod
    def split(self, metric, fields, path, keys, key_state, warnings) -> Sequence[InternalMetric]:
        pass

    def _split(self, metric, fields: Union[Sequence[Field], Field], path: str, warnings):
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
                ncontents, cchanged = yield from self._split(metric, field, path, warnings)
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
            has_key, state = self.has_key(n_path)
            if has_key:
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

        elif fields_with_children:
            # if we have children, this means we have children
            # lists or composed fields.
            for fname, n_path in fields_with_children.items():
                fcontent = fields[fname]
                # if there is no content we ignore.
                if not fcontent:
                    continue
                # we yield all internal splits, then we replace
                # the value if there was any change.
                ncontents, cchanged = yield from self._split(metric, fcontent, n_path, warnings)
                if cchanged:
                    changed = True
                    fields.pop(fname, None)
                    # not write the new field if empty
                    if ncontents:
                        fields[fname] = ncontents

        return fields, changed


class ExtraKeysTransformation(MetricSpliting):
    def split(self, metric, fields, path, new_keys, key_state, warnings):
        current_keys = metric.keys
        new_keys = metric.add_keys(current_keys, new_keys)
        current_content = fields
        for key in new_keys:
            current_content.pop(key, None)
        new_metric = metric.replace(content=current_content, keys=new_keys, path=path)
        # now, we need to apply the change also here
        yield from self.transform(new_metric)
        return {}, True


class NotAList(MetricExceptionBase):
    pass


class SplitLists(MetricSpliting):
    def split(self, metric, fields, path, new_keys, key_state, warnings):
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


class CombineTransformationSeries(MetricSpliting):
    """
    This is kind of broken implementation, since it has state. Not sure how to do it better
    without needing to pack and unpack too many times.
    """

    def __init__(self, transformations: Sequence["MetricSplit"]):
        self.transformations = transformations

    def set_warning_function(self, warning_function):
        self._warning = warning_function
        for trs in self.transformations:
            trs.set_warning_function(warning_function)

    def has_node(self, path: str) -> bool:
        """
        Checks whether an encoding path is covered by the operation
        """
        for t in self.transformations:
            if t.has_node(path):
                return True
        return False

    def has_key(self, path: str):
        """
        Checks whether the operation applies to a field
        """
        for n, t in enumerate(self.transformations):
            has, state = t.has_key(path)
            if has:
                return (True, (n, state))
        return (False, (None, None))

    def split(self, metric, fields, path, keys, key_state, warnings) -> Sequence[InternalMetric]:
        # here we apply only the first operation
        state_per_transform = {}
        for key, (n, state) in key_state.items():
            state_per_transform.setdefault(n, {})[key] = state
        min_n = min(state_per_transform)
        state = state_per_transform[min_n]
        keys_content = {x: y for x, y in keys.items() if x in state}
        value = yield from self.transform_list(
            self.transformations[min_n].split(metric, fields, path, keys_content, state, warnings)
        )
        return value


class MetricWarningDummy(MetricExceptionBase):
    pass


class MetricTransformDummy(MetricTransformationBase):
    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        warnings.append(MetricWarningDummy("Dummy warning", {"df": 2}))
        yield metric



class RemoveContentHierarchies(MetricTransformationBase):
    '''
    Forces content to be a dict, it might yield multiple metircs.
        - if it is dict, it remains.
        - if iti s a list, it yields a metric per element (and continious recursively)
    '''
    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        if isinstance(metric.content, dict):
            yield metric
        elif isinstance(metric.content, list):
            for elem in metric.content:
                new_metric = metric.replace(content=elem)
                yield from self.transform(new_metric)
        warnings.append(TransformationException("Found a type different from list or dict in content"))
        yield metric


def metric_to_json_dict(metric, content_base=True, content_key="content"):
    '''
    Returns a dict from a metric.
    The dict should be converted to json later.
    '''
    if not content_base:
        return metric.to_dict()

    content = metric.content.copy()
    if not isinstance(content, dict):
        content = {content_key: content}
    keys =None
    try:
        keys = getattr(metric,"keys", None)
    except:
        pass
    if keys is not None and isinstance(keys, dict):
        content.update(keys)
    content.update(metric.headers)
    return content


