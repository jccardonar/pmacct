from abc import ABC, abstractmethod 
from typing import Iterable, Sequence, Dict, Union, Any, Generator, Tuple
import ujson as json
from pygtrie import CharTrie

# Types
ValueField = Dict[str, Union[str, float]]
Field = Dict[str, Union["Field", ValueField]]

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
        if encoding_path[-1] == "/":
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

    def get_extra_keys(self) -> Iterable["InternalMetric"]:
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
        fields, changed = yield from self._get_extra_keys(self.content, self.path)
        if changed and fields:
            new_data = self.data.copy()
            new_data[self.content_key] = fields
            new_self = InternalMetric(new_data)
            yield new_self
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
        new_data = self.data.copy()
        current_keys = self.keys
        new_keys = self.add_keys(current_keys, new_keys)
        new_data[self.p_key] = path
        current_content = fields
        for key in new_keys:
            current_content.pop(key, None)
        new_data[self.keys_key] = new_keys
        new_data[self.content_key] = current_content
        newmetric = InternalMetric(new_data)
        yield from newmetric.get_extra_keys()



class JsonTextMetric(BaseEncoding):
    '''
    Data is a json string.
    '''
    def get_internal(self, *args, **kargs):
        new_content = json.loads(self.data.content)
        new_data = self.data.copy()
        new_data[self.content_key] = new_content
        return InternalMetric(new_data, *args, **kargs)





