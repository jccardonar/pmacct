from abc import ABC, abstractmethod
from typing import Iterable, Sequence, Dict, Union, Any, Generator, Tuple
import ujson as json
from pygtrie import CharTrie
from enum import Flag, auto

# Types
ValueField = Dict[str, Union[str, float]]
Field = Dict[str, Union["Field", ValueField]]


RANGE_NUMERS = [str(x) for x in range(0, 100)]


class BaseEncoding:
    content_key = "content"
    keys_key = "keys"
    p_key = "encodingPath"

    def __init__(self, data: Dict[Any, Any]):
        self.data = data
        try:
            self.content
        except:
            raise BaseEncodingException("Content not found")

    @abstractmethod
    def get_internal(self, *args, **kargs) -> "InternalMetric":
        """
        Gets an internal metric
        """
        pass

    def load_from_data(self, key, name=None):
        if name is None:
            name = key
        if key not in self.data:
            raise GetData(f"Error getting {name}, no key {key}")
        return self.data[key]

    @property
    def content(self):
        return self.load_from_data(self.content_key, "content")

    @property
    def keys(self):
        return self.load_from_data(self.keys_key, "keys")

    @property
    def path(self) -> str:
        """
        The path (aka as sensor path, encoding path) is the path from where you are getting the data
        """
        return self.load_from_data(self.p_key, "path")

    @staticmethod
    def form_encoding_path(encoding_path, levels):
        if not levels:
            return encoding_path
        # if there is no encoding path, then we just return the levesl
        if not encoding_path:
            return "/".join(levels)
        if encoding_path and encoding_path[-1] == "/":
            encoding_path = encoding_path[:-1]
        return "/".join([encoding_path, "/".join(levels)])

    def to_json(self):
        return json.dumps(self.data)

    @classmethod
    def from_json(cls, json_string):
        data = json.loads(json_string)
        return cls(data)


class TelemetryException(Exception):
    pass


class BaseEncodingException(TelemetryException):
    pass


class GetData(BaseEncodingException):
    pass


class InternalIteratorError(BaseEncodingException):
    pass


HIERARCHICAL_TYPES = (dict, list)


class InternalMetric(BaseEncoding):
    extra_keys: Iterable[str]

    def __init__(self, data, extra_keys=None):
        if extra_keys is None:
            extra_keys = CharTrie()
        self.extra_keys = extra_keys
        super().__init__(data)

    def replace(self, content=None, keys=None, path=None):
        new_data = self.data.copy()
        if content is not None:
            new_data[self.content_key] = content
        if keys is not None:
            new_data[self.keys_key] = keys
        if path is not None:
            new_data[self.p_key] = path
        return InternalMetric(new_data)

    def flatten_data(self) -> Iterable[Sequence[ValueField]]:
        pass

    def get_json(self):
        new_data = self.data.copy()

        new_content = json.dumps(self.content)
        new_data[self.content_key] = new_content

        if self.keys:
            new_key = json.dumps(self.keys)
            new_data[self.keys_key] = new_key

        return JsonTextMetric(new_data)

    def get_extra_keys(self, extra_keys=None) -> Iterable["InternalMetric"]:
        """
        Generator of new metrics. 
        Navigates the content, finding if there is a new key.
        If it finds one, it generates a new metric from that point (repeating the process recursively).
        It will "cut" the content from the new metrics.
        Heavy lifting is on _get_extra_keys. This one basically just starts and
        returns the modified "self", 
        checking whether the base metric should be yielded as it is or a new
        one must be constructed.
        """
        if extra_keys is not None:
            self.extra_keys = extra_keys

        fields, changed = yield from self._get_extra_keys(self.content, self.path)
        if changed:
            if fields:
                # new_data = self.data.copy()
                # new_data[self.content_key] = fields
                # new_self = InternalMetric(new_data)
                # yield new_self
                yield self.replace(content=fields)
            return
        yield self

    def transform(self, operation):
        if isinstance(operation, MetricSplit):
            yield from self.split(operation)
        yield from self.transform_contents(operation)

    def _get_extra_keys(
        self, fields: Union[Sequence[Field], Field], path: str
    ) -> Generator[Iterable["InternalMetric"], Any, Tuple[Sequence[Field], bool]]:
        """
        This function is a generator that also returns values.
        The return includes the new set of fields, and a bool marking whether
        the fields changed from the input.
        """
        changed = False

        # if the path is not included just return
        if not self.extra_keys.has_node(path):
            return fields, changed

        # if we are in a list, we check one by one the elements and create a new list with
        # the results
        if isinstance(fields, list):
            nlist = []
            for field in fields:
                ncontents, cchanged = yield from self._get_extra_keys(field, path)
                if cchanged:
                    changed = True
                if ncontents:
                    nlist.append(ncontents)
            # nothing else to do
            return nlist, changed

        # from here, fields is a "field"
        new_keys = {}
        fields_with_children = {}
        fields = fields.copy()

        # we go over the fields, checking whether we have a new key.
        # We add them all at the end
        for fname, fcontent in fields.items():
            n_path = self.form_encoding_path(path, [fname])
            if not self.extra_keys.has_node(n_path):
                continue
            if n_path in self.extra_keys:
                # we are in a new key here.
                # we dont even check for type of content.
                # Although that could break stuff.
                new_keys[fname] = fcontent

            if isinstance(fcontent, HIERARCHICAL_TYPES):
                fields_with_children[fname] = n_path

        # if we have any new keys, we need to split here. We do it and yield
        # anything new from the new metric (we ignore the fields_with_children
        if new_keys:
            # we need to split the metric here.
            changed = True
            yield from self.split_on_extra_keys(fields, path, new_keys)
            # we return empty.
            return {}, changed

        elif fields_with_children:
            # if we have children, this means we have children
            # lists or composed fields.
            for fname, n_path in fields_with_children.items():
                fcontent = fields[fname]
                # if there is no content we ignore.
                if not fcontent:
                    continue
                # we yield all internal keys, then we replace
                # the value if there was any change.
                ncontents, cchanged = yield from self._get_extra_keys(fcontent, n_path)
                if cchanged:
                    changed = True
                    fields.pop(fname, None)
                    # not write the new field if empty
                    if ncontents:
                        fields[fname] = ncontents

        return fields, changed

    def transform_contents(self, transformation):
        fields = self._transform_contents(self.content, self.path, transformation)
        yield self.replace(content=fields)

    def _transform_contents(
        self, fields: Union[Sequence[Field], Field], path: str, transformation
    ):
        """
        This function is a generator that also returns values.
        The return includes the new set of fields, and a bool marking whether
        the fields changed from the input.
        """

        # if the path is not included just return
        if not transformation.has_node(path):
            return fields

        # if we are in a list, we check one by one the elements and create a new list with
        # the results
        if isinstance(fields, list):
            nlist = []
            for field in fields:
                ncontents = self._transform_contents(field, path, transformation)
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
            n_path = self.form_encoding_path(path, [fname])
            has_key, state = transformation.has_key(n_path, fname, fcontent)
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
                ncontents = self._transform_contents(fcontent, n_path, transformation)
                fields.pop(fname, None)
                # not write the new field if empty
                if ncontents:
                    fields[fname] = ncontents

        # now convert the fields
        fields = transformation.transform(self, fields, path, new_keys, key_state)
        return fields

    def split(self, transformation):
        fields, changed = yield from self._split(
            self.content, self.path, transformation
        )
        if changed:
            if fields:
                yield self.replace(content=fields)
            return
        yield self

    def _split(self, fields: Union[Sequence[Field], Field], path: str, transformation):
        """
        This function is a generator that also returns values.
        The return includes the new set of fields, and a bool marking whether
        the fields changed from the input.
        """
        changed = False

        # if the path is not included just return
        if not transformation.has_node(path):
            return fields, changed

        # if we are in a list, we check one by one the elements and create a new list with
        # the results
        if isinstance(fields, list):
            nlist = []
            for field in fields:
                ncontents, cchanged = yield from self._split(
                    field, path, transformation
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
            n_path = self.form_encoding_path(path, [fname])
            has_key, state = transformation.has_key(n_path)
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
            changed = True
            yield from transformation.split(self, fields, path, new_keys, key_state)
            # we return empty.
            return {}, changed

        elif fields_with_children:
            # if we have children, this means we have children
            # lists or composed fields.
            for fname, n_path in fields_with_children.items():
                fcontent = fields[fname]
                # if there is no content we ignore.
                if not fcontent:
                    continue
                # we yield all internal keys, then we replace
                # the value if there was any change.
                ncontents, cchanged = yield from self._split(
                    fcontent, n_path, transformation
                )
                if cchanged:
                    changed = True
                    fields.pop(fname, None)
                    # not write the new field if empty
                    if ncontents:
                        fields[fname] = ncontents

        return fields, changed

    def add_keys(self, current_keys, extra_keys):
        new_keys = current_keys.copy()
        for key, value in extra_keys.items():
            self.add_to_flatten(new_keys, key, value)
        return new_keys

    def add_to_flatten(self, flatten_content, key, value):
        if key in flatten_content:
            current_state = flatten_content[key]
            if isinstance(current_state, list):
                current_state.append(value)
            else:
                current_list = [current_state, value]
                flatten_content[key] = current_list
            return
        flatten_content[key] = value

    def split_on_extra_keys(self, fields, path, new_keys):
        current_keys = self.keys
        new_keys = self.add_keys(current_keys, new_keys)
        current_content = fields
        for key in new_keys:
            current_content.pop(key, None)
        # new_data = self.data.copy()
        # new_data[self.p_key] = path
        # new_data[self.keys_key] = new_keys
        # new_data[self.content_key] = current_content
        # newmetric = InternalMetric(new_data)
        yield from self.replace(
            content=current_content, keys=new_keys, path=path
        ).get_extra_keys(self.extra_keys)
        # yield from newmetric.get_extra_keys(self.extra_keys)

    def flatten(self, flatten_config=None):
        """
        splits: any transformation that splits. Different encoding paths. Same tranformation can be applied without loops.
        transforms: any tranformation that returns only one. Same transformation not applied normaly on top.
        modify_keys (flatten keys) -> transform
        extra_keys -> split on extra keys. splits.
        lists_to_strings -> set of lists that should be turned into strings. Transforms.
        flatten_lists -> sends elements of lists into itw own metric. Splits.
        rename_values -> trasnforms
        flatten_hierarchies -> remove hierrachies. transforms.
        """
        pass

    def value_to_string(self, value):
        return json.dumps(value)

    def lists_to_strings(self, fields, keys, path, new_keys):
        for key in keys:
            value = fields[key]
            new_value = self.value_to_string(value)
            fields[key] = new_value
        return fields
        yield

    def flatten_lists(self, fields, path, list_keys):
        for key in list_keys:
            for instance in fields[key]:
                new_data = self.data.copy()
                new_data[self.p_key] = path
                new_data[self.content_key] = instance
                newmetric = InternalMetric(new_data)
                yield newmetric


class MetricExceptionBase(Exception):
    def __init__(self, msg, params):
        super().__init__(msg)
        self.params = params

    def str_with_params(self):
        return f"{super().__str__()}, params{str(self.params)}"


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
        self._warning(warning)

    @abstractmethod
    def transform(self, metric) -> Sequence["InternalMetric"]:
        pass

class OptionsFields(Flag):
    NO_OPTION = 0
    LISTS = auto()
    FIELDS = auto() # a field is another field level.
    HIERARCHIES = LISTS | FIELDS

class ContentTransformation(MetricTransformationBase):
    '''
    General classes that transform content (do not yield multiple,metrics, only one transformed)
    It first transforms the hierachically fields.
    '''

    def __init__(self, options, paths):
        self.options = OptionsFields.NO_OPTION
        self.transform_list_elements = True
        for option_str in options:
            option = getattr(OptionsFields, option_str)
            self.options =  self.options | option
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


class FlattenHierarchies(ContentTransformation):

    def __init__(self, keep_naming=False):
        super().__init__([], None)
        self.keep_naming = keep_naming
        self.transform_list_elements = True

    def has_node(self, path: str):
        """
        Checks whether an encoding path is covered by the operation
        """
        return True

    def has_key(self, path: str, key, content) -> bool:
        """
        Checks whether the operation applies to a field, and returns state that is needed later
        """
        if isinstance(content, HIERARCHICAL_TYPES):
            return (True, path)
        return (False, path)

    def transform_content(self, metric, fields, path, new_keys, key_state):
        new_fields = fields.copy()
        # the sorted makes this a bit more deterministic
        for key in sorted(new_keys):
            # all keys are the hierarchical ones.
            value = new_fields.pop(key)
            kpath = key_state[key]

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
                    raise ExistingNameInFlattening("Find name returned an existing value", {"name": nname, "path": kpath, "child_key": ckey})
                new_fields[nname] = cvalue
        return new_fields


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
            self.warning(ExistingNameInFlattening("Base name for flattening exists", {"base": base_name, "path": path, "child_key": ckey}))
            for x in RANGE_NUMERS:
                candidate = "_".join([base_name, str(x)])
                if candidate not in fields:
                    break
            else:
                raise ExistingNameInFlattening("Cound not Find a name for hierarchy", {"path": path, "child_key": ckey})
            # find a name with the structure prefix_name_number, but complaint.
            name = candidate
        else:
            name = base_name

        return name

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
        global_has_key  = False
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
            new_fields = transformation.transform_content(metric, new_fields, path, state, state)
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
            if new_key in new_fields:
                self.warning(ExsitingName("Name already exists in path", {"path": path, "name": new_key}))
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
                raise RKEWrongType("Key value is not a list but modification is a dict", {"key": key, "path": metric.path})
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

    def transform_list(self, generator):
        generagtor_with_return  = Generator(generator)
        for metric in generagtor_with_return:
            yield from self.transform(metric)
        return generagtor_with_return.value

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
            new_content, changed = yield from self.split(metric, fields, path, new_keys, key_state)
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


class MetricFunction(ABC):
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
        try:
            self._warning(warning)
        except:
            # we will pass, do not know what else to do
            pass


class MetricSplit(MetricFunction):
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

    def transform_list(self, generator):
        for metric in generator:
            yield from self.transform(metric)


class ExtraKeysTransformation(MetricSpliting):
    def split(self, metric, fields, path, new_keys, key_state):
        current_keys = metric.keys
        new_keys = metric.add_keys(current_keys, new_keys)
        current_content = fields
        for key in new_keys:
            current_content.pop(key, None)
        # new_data = self.data.copy()
        # new_data[self.p_key] = path
        # new_data[self.keys_key] = new_keys
        # new_data[self.content_key] = current_content
        # newmetric = InternalMetric(new_data)
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
                self.warning(NotAList("Trying to split an element which is not a list", {"key": key}))
                continue
            for element in elements:
                new_metric = metric.replace(content=element, path=kpath)
                yield from self.transform(new_metric)
        return current_content, True
        # new_data = self.data.copy()
        # new_data[self.p_key] = path
        # new_data[self.keys_key] = new_keys
        # new_data[self.content_key] = current_content
        # newmetric = InternalMetric(new_data)

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
        value  =  yield from self.transform_list(
            self.transformations[min_n].split(metric, fields, path, keys_content, state)
        )
        return value


class MetricTransform(MetricFunction):
    def __init__(self, data_per_path):
        self.data_per_path = data_per_path

    def has_node(self, path: str):
        """
        Checks whether an encoding path is covered by the operation
        """
        return (self.data_per_path.has_node(path) > 0, None)

    def has_key(self, path: str, key, content) -> bool:
        """
        Checks whether the operation applies to a field, and returns state that is needed later
        """
        return (path in self.data_per_path, None)

    @abstractmethod
    def transform(
        self, metric, fields, path, keys, key_state
    ) -> Sequence[InternalMetric]:
        pass

    def transform_list(self, generator):
        for metric in generator:
            yield from metric.transform(self)

    def set_warning_function(self, warning_function):
        self._warning = warning_function

    def warning(self, warning):
        """
        Warnings are msg per packet. They could be logged or used in metrics if needed.
        """
        if self._warning is None:
            return
        try:
            self._warning(warning)
        except:
            # we will pass, do not know what else to do
            pass


class MetricWarningDummy(MetricExceptionBase):
    pass


class MetricTransformDummy(MetricTransform):
    def transform(self, metric):
        self.warning(MetricWarningDummy("Dummy warning", {"df": 2}))
        yield metric

    def has_node(self, path):
        return True

    def has_key(self, path: str, key, content) -> bool:
        return False, None





def TransformationChangeLeaf(MetricSplit):
    @abstractmethod
    def _key_value_transform(self, fields, keys):
        pass

    def transform(self, metric, fields, path, keys, key_state):
        new_fields = self._key_value_transform(fields, keys)
        yield metric.replace(content=new_fields)


def CombineChangeLeafs(TransformationChangeLeaf):
    def __init__(self, transformations: Sequence[MetricSplit]):
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
        return False

    def _key_value_transform(self, fields, keys, key_state):
        # here we apply only the first operation
        state_per_transform = {}
        for key, (n, state) in key_state.items():
            state_per_transform.setdefault(n, {})[key] = state
        for n in sorted(state_per_transform):
            state = state_per_transform[n]
            keys_content = {x: y for x, y in keys.items() if x in state}
            fields = self.transformations[n]._key_value_transform(fields, keys_content)
        return fields


def CombineTransformationChangeLeafs(CombineTransformationSeries):
    """
    Different from CombineTransformationSeries, this one mixes transofrmations
    that do not generate new ones and that can be applied at the same time
    """

    def transform(
        self, fields, path, keys, key_state
    ) -> Generator[InternalMetric, Any, Tuple[Sequence[Field], bool]]:
        # here we apply only the first operation
        state_per_transform = {}
        for key, (n, state) in key_state.items():
            state_per_transform.setdefault(n, {})[key] = state
        for n in sorted(state_per_transform):
            state = state_per_transform[n]
            keys_content = {x: y for x, y in keys.items() if x in state}
            # We TRUST that nothing is yielded
            nfields = yield from self.transformations[n].transform(
                fields, path, keys_content, state
            )
        yield from self.transformations[min_n].transform(
            fields, path, keys_content, state
        )

    def set_warning_function(self, warning_function):
        self._warning = warning_function
        for trs in self.transformations:
            trs.set_warning_function(warning_function)

class JsonTextMetric(BaseEncoding):
    """
    Data is a json string.
    """

    def get_internal(self, *args, **kargs):
        new_content = json.loads(self.data.content)
        new_data = self.data.copy()
        new_data[self.content_key] = new_content
        return InternalMetric(new_data, *args, **kargs)
