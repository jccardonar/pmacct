from encoders.base import ValueField, Field, InternalMetric, MetricExceptionBase
from typing import Iterable, Sequence, Dict, Union, Any, Generator, Tuple
from abc import ABC, abstractmethod
from enum import Flag, auto
import ujson as json
from pygtrie import CharTrie

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

class MetricTransformationBase(ABC):
    def __init__(self, data_per_path):
        self.data_per_path = data_per_path
        self._warning = None

    def set_warning_function(self, warning_function):
        self._warning = warning_function

    def warning(self, warning):
        """
        Warnings are msg per packet. They could be logged or used in metrics if needed.
        """
        if self._warning is None:
            return

        # should we catch a failure here, what to do?
        try:
            self._warning(warning)
        except:
            pass

    @abstractmethod
    def transform(self, metric) -> Sequence["InternalMetric"]:
        pass

    def transform_list(self, generator):
        """
        Takes a generator of metrics and transforms. It keeps the value in case one needs
        to do  "= yield from"
        """
        generagtor_with_return = Generator(generator)
        for metric in generagtor_with_return:
            yield from self.transform(metric)
        return generagtor_with_return.value


class TransformationPerEncodingPath(MetricTransformationBase):
    """
    Applies a transformation per encoding path. Used to quickly filter paths.
    """
    def __init__(self, transformation_per_path, default):
        self.transformation_per_path = transformation_per_path
        self.default = default
        super().__init__(None)

    def transform(self, metric):
        # here, it would be possible to have multiple cases. But that is not the idea.
        transformation = self.transformation_per_path.get(metric.path, self.default)
        if transformation:
            yield from transformation.transform(metric)
        else:
            yield metric


class FilterMetric(MetricTransformationBase):
    """
    We just filter by encoding path. Other more complex filters can be done,
    but we dont need the now.
    """

    def transform(self, metric):
        if metric.path in self.data_per_path:
            return
        yield metric


class OptionsFields(Flag):
    NO_OPTION = 0
    LISTS = auto()
    FIELDS = auto()  # a field is another field level.
    HIERARCHIES = LISTS | FIELDS


class ContentTransformation(MetricTransformationBase):
    """
    General classes that transform content (do not yield multiple,metrics, only one transformed)
    It first transforms the hierachically fields.
    """

    def __init__(self, options, paths):
        self.options = OptionsFields.NO_OPTION
        self.transform_list_elements = True
        for option_str in options:
            option = getattr(OptionsFields, option_str)
            self.options = self.options | option
        super().__init__(paths)

    def has_node(self, path):
        if self.options.value > 0:
            # we have options, we need to look at everything.
            return True
        return self.data_per_path.has_node(path) > 0

    def has_key(self, path, key, value):
        if self.options.value > 0:
            if isinstance(value, list) and OptionsFields.LISTS in self.options:
                return (True, path)
            if isinstance(value, dict) and OptionsFields.FIELDS in self.options:
                return (True, path)
        if path in self.data_per_path:
            return (True, path)
        return (False, path)

    def transform(self, metric):
        fields = self._transform_contents(metric, metric.content, metric.path)
        yield metric.replace(content=fields)

    def _transform_contents(
        self, metric, fields: Union[Sequence[Field], Field], path: str
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
                ncontents = self._transform_contents(metric, field, path)
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
            # if we have children, this means we have children
            # lists or composed fields.
            for fname, n_path in fields_with_children.items():
                fcontent = fields[fname]
                # if there is no content we ignore.
                if not fcontent:
                    continue
                # we yield all internal keys, then we replace
                # the value if there was any change.
                ncontents = self._transform_contents(metric, fcontent, n_path)
                fields.pop(fname, None)
                # not write the new field if empty
                if ncontents:
                    fields[fname] = ncontents

        # now convert the fields, only if there are matches.
        if new_keys:
            fields = self.transform_content(metric, fields, path, new_keys, key_state)
        return fields

    @abstractmethod
    def transform_content(metric, fields, path, new_keys, key_state):
        pass


class FieldToString(ContentTransformation):
    @staticmethod
    def string_field(value):
        if isinstance(value, str):
            new_value = value
            return new_value
        return json.dumps(value)

    def transform_content(self, metric, fields, path, new_keys, key_state):
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

    def find_name(self, fields, key, ckey, path) -> str:
        prefix = ""
        if self.keep_naming:
            prefix = key
        if prefix:
            base_name = "_".join([prefix, ckey])
        else:
            base_name = ckey
        name = None
        if base_name in fields:
            self.warning(
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

    def transform(self, metric):
        gen = iter([metric])
        for trf in self.transformations:
            gen = trf.transform_list(gen)
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

    def add_to_field(self, fields, key, value, path):
        nname = self.find_name(fields, "keys", key, path)
        if nname in fields:
            raise ExistingNameInFlattening(
                "Find name returned an existing value",
                {"name": nname, "path": path, "child_key": key},
            )
        fields[nname] = value

    def transform(self, metric):
        new_fields = {}
        self.flatten_keys(metric.keys, metric.path, new_fields)
        self.add_to_field(
            new_fields, metric.timestamp_key, metric.timestamp, metric.path
        )
        self.add_to_field(new_fields, metric.node_key, metric.node, metric.path)

        # now the content
        for key, value in metric.content.items():
            self.add_to_field(new_fields, key, value, metric.path)

        yield metric.replace(content=new_fields)

    def flatten_keys(self, keys, path, new_fields):
        for key, value in keys.items():
            if isinstance(value, list):
                for svalue in value:
                    self.add_to_field(new_fields, key, value, path)
                self.warning(
                    KeysWithDoubleName(
                        "Keys with the same name", {"name": key, "path": path}
                    )
                )
                continue
            self.add_to_field(new_fields, key, value, path)


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

    def transform_content(self, metric, fields, path, new_keys, key_state):
        new_fields = fields.copy()
        # the sorted makes this a bit more deterministic
        for key in sorted(new_keys):
            # all keys are the hierarchical ones.
            value = new_fields.pop(key)
            kpath = key_state[key]

            if not isinstance(value, HIERARCHICAL_TYPES):
                self.warning(
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
                self.warning(FlattenningLists("Flattening lists", {"path": kpath}))
                nvalue = FieldToString.string_field(value)
                new_fields[key] = nvalue
                continue

            # this must be a dict, that should be already flatten
            for ckey, cvalue in value.items():
                nname = self.find_name(new_fields, key, ckey, kpath)
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

    def transform_content(self, metric, fields, path, new_keys, key_state):
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
                metric, new_fields, path, state, state
            )
        return new_fields


class ExsitingName(MetricExceptionBase):
    pass


class RenameContent(ContentTransformation):
    def __init__(self, paths):
        super().__init__([], paths)

    def transform_content(self, metric, fields, path, new_keys, key_state):
        new_fields = fields.copy()
        for key in new_keys:
            path = key_state[key]
            new_key = self.data_per_path[path]
            if new_key is None:
                new_fields.pop(key, None)
                continue
            if new_key in new_fields:
                self.warning(
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
    def transform(self, metric):
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


# from https://stackoverflow.com/questions/34073370/best-way-to-receive-the-return-value-from-a-python-generator
class Generator:
    def __init__(self, gen):
        self.gen = gen

    def __iter__(self):
        self.value = yield from self.gen


class MetricSpliting(MetricTransformationBase):
    """
    Base functioning of splitting the metric similar to how the extrq keys function. Other splitting operations might have the same characteristics.
    """

    def transform(self, metric):
        fields, changed = yield from self._split(metric, metric.content, metric.path)
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
    def split(self, metric, fields, path, keys, key_state) -> Sequence[InternalMetric]:
        pass

    def _split(self, metric, fields: Union[Sequence[Field], Field], path: str):
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
                ncontents, cchanged = yield from self._split(metric, field, path)
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
                metric, fields, path, new_keys, key_state
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
                ncontents, cchanged = yield from self._split(metric, fcontent, n_path)
                if cchanged:
                    changed = True
                    fields.pop(fname, None)
                    # not write the new field if empty
                    if ncontents:
                        fields[fname] = ncontents

        return fields, changed


class ExtraKeysTransformation(MetricSpliting):
    def split(self, metric, fields, path, new_keys, key_state):
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
    def split(self, metric, fields, path, new_keys, key_state):
        current_content = fields
        for key in new_keys:
            kpath = key_state[key]
            elements = current_content.pop(key, None)
            if not isinstance(elements, list):
                self.warning(
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

    def split(self, metric, fields, path, keys, key_state) -> Sequence[InternalMetric]:
        # here we apply only the first operation
        state_per_transform = {}
        for key, (n, state) in key_state.items():
            state_per_transform.setdefault(n, {})[key] = state
        min_n = min(state_per_transform)
        state = state_per_transform[min_n]
        keys_content = {x: y for x, y in keys.items() if x in state}
        value = yield from self.transform_list(
            self.transformations[min_n].split(metric, fields, path, keys_content, state)
        )
        return value


class MetricWarningDummy(MetricExceptionBase):
    pass


class MetricTransformDummy(MetricTransformationBase):
    def transform(self, metric):
        self.warning(MetricWarningDummy("Dummy warning", {"df": 2}))
        yield metric


