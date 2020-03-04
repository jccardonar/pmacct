from abc import ABC, abstractmethod 
from typing import Iterable, Sequence, Dict, Union, Any, Generator, Tuple
import ujson as json
from pygtrie import CharTrie

# Types
ValueField = Dict[str, Union[str, float]]
Field = Dict[str, Union["Field", ValueField]]


RANGE_NUMERS  = [str(x) for x in range(0, 100)]

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
        '''
        Gets an internal metric
        '''
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
        '''
        The path (aka as sensor path, encoding path) is the path from where you are getting the data
        '''
        return self.load_from_data(self.p_key, "path")

    @staticmethod
    def form_encoding_path(encoding_path, levels):
        if not levels:
            return encoding_path
        # if there is no encoding path, then we just return the levesl
        if not encoding_path:
            return '/'.join(levels)
        if encoding_path and encoding_path[-1] == "/":
            encoding_path = encoding_path[:-1]
        return '/'.join([encoding_path, '/'.join(levels)])

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

class InternalMetric(BaseEncoding):
    HIERARCHICAL_TYPES = (dict, list)
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
        '''
        Generator of new metrics. 
        Navigates the content, finding if there is a new key.
        If it finds one, it generates a new metric from that point (repeating the process recursively).
        It will "cut" the content from the new metrics.
        Heavy lifting is on _get_extra_keys. This one basically just starts and
        returns the modified "self", 
        checking whether the base metric should be yielded as it is or a new
        one must be constructed.
        '''
        if extra_keys is not None:
            self.extra_keys = extra_keys

        fields, changed = yield from self._get_extra_keys(self.content, self.path)
        if changed:
            if fields:
                #new_data = self.data.copy()
                #new_data[self.content_key] = fields
                #new_self = InternalMetric(new_data)
                #yield new_self
                yield self.replace(content=fields)
            return
        yield self


    def _get_extra_keys(self, fields: Union[Sequence[Field], Field], path:str) -> Generator[Iterable["InternalMetric"], Any, Tuple[Sequence[Field], bool]]:
        '''
        This function is a generator that also returns values.
        The return includes the new set of fields, and a bool marking whether
        the fields changed from the input.
        '''
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
        for fname, fcontent  in fields.items():
            n_path = self.form_encoding_path(path, [fname])
            if not self.extra_keys.has_node(n_path):
                continue
            if n_path in self.extra_keys:
                # we are in a new key here.
                # we dont even check for type of content. 
                # Although that could break stuff.
                new_keys[fname] = fcontent

            if isinstance(fcontent, self.HIERARCHICAL_TYPES):
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

    def transform(self, transformation):
        fields, changed = yield from self._transform(self.content, self.path, transformation)
        if changed:
            if fields:
                yield self.replace(content=fields)
            return
        yield self

    def _transform(self, fields: Union[Sequence[Field], Field], path:str, transformation):
        '''
        This function is a generator that also returns values.
        The return includes the new set of fields, and a bool marking whether
        the fields changed from the input.
        '''
        changed = False

        # if the path is not included just return
        if not transformation.has_node(path):
            return fields, changed

        # if we are in a list, we check one by one the elements and create a new list with 
        # the results
        if isinstance(fields, list):
            nlist = []
            for field in fields:
                ncontents, cchanged = yield from self._transform(field, path, transformation)
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
        for fname, fcontent  in fields.items():
            n_path = self.form_encoding_path(path, [fname])
            if not transformation.has_node(n_path):
                continue
            has_key, state = transformation.has_key(n_path)
            if has_key:
                # we are in a new key here.
                # we dont even check for type of content. 
                # Although that could break stuff.
                new_keys[fname] = fcontent
                key_state[fname] = state

            if isinstance(fcontent, self.HIERARCHICAL_TYPES):
                fields_with_children[fname] = n_path


        # if we have any new keys, we need to split here. We do it and yield
        # anything new from the new metric (we ignore the fields_with_children
        if new_keys:
            # we need to split the metric here.
            changed = True
            yield from transformation.transform(self, fields, path, new_keys, key_state)
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
                ncontents, cchanged = yield from self._transform(fcontent, n_path, transformation)
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
        #new_data = self.data.copy()
        #new_data[self.p_key] = path
        #new_data[self.keys_key] = new_keys
        #new_data[self.content_key] = current_content
        #newmetric = InternalMetric(new_data)
        yield from self.replace(content=current_content, keys=new_keys, path=path).get_extra_keys(self.extra_keys)
        #yield from newmetric.get_extra_keys(self.extra_keys)

    def flatten(self, flatten_config=None):
        '''
        extra_keys -> split on extra keys
        lists_to_strings -> set of lists that should be turned into strings.
        flatten_lists -> sends elements of lists into itw own metric
        flatten_keys
        flatten_hierarchies -> remove hierrachies

        '''
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

    def flatten_hierarchies(self, fields, keys, keep_naming=False):
        for key in keys:
            values = fields[key]
            for value in values:
                if isinstance(fcontent, dict):
                    newvalue = self.flatten_content(self, fields, levels)
                    fields[key] = newvalue
                elif isinstance(fcontent, list):
                    # we dont do this well, we just convert to string
                    # complain here.
                    complain
                    newvalue = self.lists_to_strings()
                    fields[key] = newvalue
                else:
                    new_name = self.find_name(fiedls, value, keep_naming)
                    fields[new_name] = value
    
    def find_name(self, fields, field, keep_naming, levels):
        prefix = ""
        if keep_naming:
            prefix = level
        # the sorted makes this a bit more deterministic
        for value in sorted(values):
            base_name = '_'.join([prefix, value])
            if base_name in fields:
                for x in RANGE_NUMERS:
                    candidate = '_'.join([base_name, str(x)])
                    if candidate not in fields:
                        break
                else:
                    raise Exception
                # find a name with the structure prefix_name_number, but complaint.
                complain
                name = candidate
            else:
                name = base_name
            fields[name] = value
        fields.pop(name, None)



class MetricTransformation(ABC):

    def __init__(self, data_per_path):
        self.data_per_path = data_per_path

    def has_node(self, path: str) -> bool:
        '''
        Checks whether an encoding path is covered by the operation
        '''
        return self.data_per_path.has_node(path) > 0

    def has_key(self, path: str) -> bool:
        '''
        Checks whether the operation applies to a field, and returns state that is needed later
        '''
        return (path in self.data_per_path, None)
    
    @abstractmethod
    def transform(self, metric, fields, path, keys, key_state) -> Sequence[InternalMetric]:
        pass

    def tranform_list(self, generator):
        for metric in generator:
            yield from metric.transform(self)

class ExtraKeysTransformation(MetricTransformation):
    def transform(self, metric, fields, path, new_keys, key_state):
        current_keys = metric.keys
        new_keys = metric.add_keys(current_keys, new_keys)
        current_content = fields
        for key in new_keys:
            current_content.pop(key, None)
        #new_data = self.data.copy()
        #new_data[self.p_key] = path
        #new_data[self.keys_key] = new_keys
        #new_data[self.content_key] = current_content
        #newmetric = InternalMetric(new_data)
        yield from metric.replace(content=current_content, keys=new_keys, path=path).transform(self)

class CombineTransformationSeries(MetricTransformation):
    '''
    This is kind of broken implementation, since it has state. Not sure how to do it better
    without needing to pack and unpack too many times.
    '''
    def __init__(self, transformations: Sequence["MetricTransformation"]):
        self.transformations = transformations

    def has_node(self, path: str) -> bool:
        '''
        Checks whether an encoding path is covered by the operation
        '''
        for t in self.transformations:
            if t.has_node(path):
                return True
        return False

    def has_key(self, path: str):
        '''
        Checks whether the operation applies to a field
        '''
        for n, t in enumerate(self.transformations):
            has, state = t.has_key(path)
            if has:
                return (True, (n, state))
        return (False, (None, None))

    
    def transform(self, metric, fields, path, keys, key_state) -> Sequence[InternalMetric]:
        # here we apply only the first operation
        state_per_transform = {}
        for key, (n, state) in key_state.items():
            state_per_transform.setdefault(n, {})[key] = state
        min_n = min(state_per_transform)
        state = state_per_transform[min_n]
        keys_content = {x: y for x,y in keys.items() if x in state}
        yield from self.tranform_list(self.transformations[min_n].transform(metric, fields, path, keys_content, state))


def TransformationChangeLeaf(MetricTransformation):
    @abstractmethod
    def _key_value_transform(self, fields, keys):
        pass

    def transform(self, metric, fields, path, keys, key_state):
        new_fields = self._key_value_transform(fields, keys)
        yield metric.replace(content=new_fields)

def CombineChangeLeafs(TransformationChangeLeaf):

    def __init__(self, transformations: Sequence[MetricTransformation]):
        self.transformations = transformations

    def has_node(self, path: str) -> bool:
        '''
        Checks whether an encoding path is covered by the operation
        '''
        for t in self.transformations:
            if t.has_node(path):
                return True
        return False

    def has_key(self, path: str):
        '''
        Checks whether the operation applies to a field
        '''
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
            keys_content = {x: y for x,y in keys.items() if x in state}
            fields = self.transformations[n]._key_value_transform(fields, keys_content)
        return fields


def CombineTransformationChangeLeafs(CombineTransformationSeries):
    '''
    Different from CombineTransformationSeries, this one mixes transofrmations
    that do not generate new ones and that can be applied at the same time
    '''
    
    def transform(self, fields, path, keys, key_state) -> Generator[InternalMetric, Any, Tuple[Sequence[Field], bool]]:
        # here we apply only the first operation
        state_per_transform = {}
        for key, (n, state) in key_state.items():
            state_per_transform.setdefault(n, {})[key] = state
        for n in sorted(state_per_transform): 
            state = state_per_transform[n]
            keys_content = {x: y for x,y in keys.items() if x in state}
            # We TRUST that nothing is yielded
            nfields = yield from self.transformations[n].transform(fields, path, keys_content, state)
        yield from self.transformations[min_n].transform(fields, path, keys_content, state)


class JsonTextMetric(BaseEncoding):
    '''
    Data is a json string.
    '''
    def get_internal(self, *args, **kargs):
        new_content = json.loads(self.data.content)
        new_data = self.data.copy()
        new_data[self.content_key] = new_content
        return InternalMetric(new_data, *args, **kargs)





