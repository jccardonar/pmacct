'''
The code here attempts to follow the structure from the public schema from cisco, but it could very well vary across platforms. We use https://github.com/cisco/bigmuddy-network-telemetry-proto/blob/master/staging/telemetry.proto as reference.

The next text attempts to describe in words the cisco kv format. Please see the reference above for a better understanding.

The cisco-kv encoding is a "generic" way of encoding any hierarchical metric. It defines a TelemetryField that can contains as fields a timestamp, a name, a value, and a field called "fields" (which contains other "children" TelemetryFields - making TelemetryFields recursive-). The value is actually not a single field, but an option value_by_type with a different name depending on the type.

Lists in grpc self-describe are not easily marked. They are just fields with same names in the same hierarchy.

The flattening is a two step process. In the firt pass we convert the grcoc into an internal structure

Values = Union[str, float, int]
Dict[str, Union[Values, List["field"]]]

In the process, we check for lists using the encoding path info. If a repeated key appears in a non-list, there is an error. If there is nothing, then . 

Repeated keys are also converted into lists (but the order is preserved).

Extra keys are also mmodifed here. When there is a point with an extra key, A new metric is generated.
New encoding paths are also marked and done differntly.

For flattening. The process goes:

For keys, repeated values are taken from a file. If nothing is given, a number is appended to the end.

For content:
    if a container is marked as ignored, we just bring the other hierarchy down.
    A continaer can also be makred as string, in which the contents will becomes strings.
    Lists will spam a new metri (copying everything on top). If you need to join themm, this wil lhave to happend in later stages of the pipelines (or use the tipical hierrachical output for that encoding path). Normally, complex data structures are eithre signs of problems with the model, or that you are trying to obtain data structures that are not simple metrics (an not suitable for TSDBs).



Note that we generate from the omdel some of these values using another command.


The schema does not forbid it, but we assume that a TelemetryField should populate either the value or the children fields. We will call fields with a value "value fields" and fields with children "containers".

In Cisco KV, the first TelemetryField (located in data_gpbkv on the main Telemetry msg) should contain 2 fields: one with the name "keys" and other with the name "content".

The "keys" fields should only contain value fields. These will be the keys of the encoding path. Note that this forces encoding paths to not contain a listed key within them. 

The "content" fields can be hierarchical (contain value and containers). 

Many TSDBs or storing systems do not support hierarchical data, but simple metrics. A metric in this context is a set of values fields. Keys are not even marked in the data, but known in advanced in the system. This code attempts to flatten the structure of the cisco kv. The basic idea here is to:

- Note that the operation can multiply the number of metrics (from a hierarchical metric, multiple flatten ones  can be produced). 
- if there is a container, flatten it recursively. This can result in multiple tuples, as mentioned. For each of them, remove the nesting (bring the values down to the top hierarchy). To avoid naming collision, the name of the container is prepended to the names of the keys. If there is still a conflict, warn, and append _n, where increments until a conflict is avoided.
- For lists, we have two ways of doing it. The method  here is not perfect.
    - We define simple lists as those that just contain only name-values, similar to leaf-lists. We flatten themconverting them to strings and separating them with a _ value. This is not perfect, but simplifies the whole thing. 
    - Compound lists are those containing more than one value type. Here we generate a metric per element of the list.

There is a bit of a caveat on the hierrachical metric. Some models are missing keys in the deeper hierarchies. This happens, for instance, in the QoS model of cisco. This code accepts "manual" definitions of keys in the structure. When a key in the middle of the structure is found, the higher hierarchies are ignored, we flatten the structure from there, but we do append the keys from the lower ones.

Another general operation is modifying the name of the fields (mostly AFTER the changes due to hierrachy).
'''



class CiscoKVFlatten():
    def __init__(self, names_data=None, extra_keys=None):
        if names_data is None:
            names_data = {}
        if extra_keys is None:
            extra_keys = {}
        self.names_data = names_data
        self.extra_keys = extra_keys


    def convert_grpc_to_dict(self, fields):
        keys = None
        content = None
        for field in fields:
            if field.get("name", "") == "keys":
                keys = field
            elif field.get("name", "") == "content":
                content = field
        if keys is None:
            raise Exception("No keys in field {}".format(fields))
        if content is None:
            raise Exception("No content in field {}".format(fields))
        metric_keys = {}

        # flatten keys
        for n, key in enumerate(keys["fields"]):
            key, value = self.simplify_cisco_field(key)
            self.add_to_flatten(metric_keys, key, value)

        flatten_content = self.convert_grpc_to_dict_content(content)
        return metric_keys, flatten_content

    def add_to_flatten(self, flatten_content, key, value):
        if key in flatten_content:
            if key == "cac-state":
                df
            current_state = flatten_content[key]
            if isinstance(current_state, list):
                current_state.append(value)
            else:
                current_list = [current_state, value]
                flatten_content[key] = current_list
            return
        flatten_content[key] = value




    def convert_grpc_to_dict_content(self, content_f):
        flatten_content = {}
        for field in content_f["fields"]:
            if "fields" in field and field["fields"]:
                name = field.get("name", None)
                if name is None:
                    name = "Unknown"
                value = self.convert_grpc_to_dict_content(field)
            else:
                name, value = self.simplify_cisco_field(field)
            self.add_to_flatten(flatten_content, name, value)
        return flatten_content




    def flatten_cisco_fields(self, fields, encoding_path, flatten_array=None):
        """
        Takes data and returns one or more flatten jsons.
        """
        if flatten_array is None:
            flatten_array = []
        # let's take care of the key-content type of cisco
        if self.is_key_value(fields):
            yield from self.flatten_key_content(fields, encoding_path, flatten_array)
        else:
            # we might have multiple keys, let's just take them one by one
            for field in fields:
                yield from self.flatten_cisco_fields(field, encoding_path, flatten_array=flatten_array)

    def flatten_key_content(self, fields, encoding_path, metrics):
        # get keys and content
        keys = None
        content = None
        for field in fields:
            if field.get("name", "") == "keys":
                keys = field
            elif field.get("name", "") == "content":
                content = field
        if keys is None:
            raise Exception("No keys in field {}".format(fields))
        if content is None:
            raise Exception("No content in field {}".format(fields))
        metric_keys = {}

        # flatten keys
        for n, key in enumerate(keys["fields"]):
            key, value = self.simplify_cisco_field(key, encoding_path=encoding_path, key_n=n)
            metric_keys[key] = value
        flatten_metrics = []
        self.flatten_content_fields(content, encoding_path, metric_keys, flatten_metrics, metrics)
        # now we can create the multiple metrics from a single one, if needed
        #for content_f in content["fields"]:
            #if "fields" in content_f and content_f["fields"]:
            #    breakpoint()
            #    raise Exception("Not ready")
            #key, value = simplify_cisco_field(content_f)
            #flatten_metric[key] = value
        metrics.extend(combine_keys_content(metric_keys, flatten_metrics))
        return metrics

    def flatten_content_fields(self, content_f, encoding_path, keys, flatten_metrics, other_metrics, level=None):
        '''
        Here we have pure content.
        '''
        if level is None:
            level = []
        # first we go over elements colleting all "normal" in this hierarchy
        fields_with_children = []
        this_encoding_path = form_encoding_path(encoding_path, level)
        look_for_keys = this_encoding_path in self.extra_keys
        flatten_metric = {}
        for field in content_f["fields"]:
            if "fields" in field and field["fields"]:
                fields_with_children.append(field)
                continue
            name, value = self.simplify_cisco_field(field, encoding_path=encoding_path, levels=level)
            if look_for_keys:
                if name in self.extra_keys[this_encoding_path]:
                    keys[name] = value
                else:
                    flatten_metric[name] = value
            else:
                flatten_metric[name] = value

        children_flatten_metrics = []
        if fields_with_children:
            breakpoint()
            for field in fields_with_children:
                name = field.get("name", None)
                if name is None:
                    name = "Unknown"
                new_levels = level + [name]
                if add_leaf(this_encoding_path, name) in self.extra_keys:
                    raise Exception("Not ready")
                    new_keys = dict(keys)
                    child_flatten_content = []
                    self.flatten_content_fields(field, this_encoding_path, new_keys, child_flatten_content, other_metrics)
                    # now gett the flatten value and add it
                    new_metric = combine_keys_content(new_keys, child_flatten_content)
                    other_metrics.append(new_metric)
                else:
                    self.flatten_content_fields(field, encoding_path, keys, children_flatten_metrics, other_metrics, new_levels)
                    # our metrics are the ones in chilren_flatten_metrics together with the ones in this hierarchy
        if children_flatten_metrics:
            for children_metric in children_flatten_metrics:
                this_metric = dict(flatten_metric)
                this_metric.update(children_metric)
                flatten_metrics.append(this_metric)
        else:
            flatten_metrics.append(flatten_metric)


    @staticmethod
    def is_key_value(fields) -> bool:
        """
        Returns yes if the field is of type key_content
        """
        return len(fields) == 2 and set(
            [field.get("name", "") for field in fields]
        ) == set(["keys", "content"])

    def simplify_cisco_field(self, field, encoding_path=None, levels=None, key_n=None):
        # find the name, this becomes more of a problem when the mapping is complicated 
        name = None
        if encoding_path in self.names_data:
            if key_n is not None:
                name = self.names_data[encoding_path]["names"][0][key_n]
            else:
                name = "_".join([*levels, field["name"]])

        if name is None:
            # problem, log
            name = field["name"]

        value = self.cast_value(field)

        return name, value

    @staticmethod
    def cast_value(field):
        '''
        Obtains a value from a TelemetryField. 
        '''
        value = None
        found = False
        found_attr = None
        for attr in field:
            if attr in NON_VALUE_FIELDS:
                continue
            if attr in ONE_OF:
                found_attr = attr
                found = True
                value = field[attr]
                break

        if not found:
            raise Exception("We could not find a way of simplifying {}".format(field))

        # try to cast value.
        casting = None
        if found_attr in INTEGERS:
            casting = int
        elif found_attr in FLOAT:
            casting = float
        elif found_attr == "boolValue":
            casting = bool

        try:
            value = casting(value)
        except:
            pass

        return value

NON_VALUE_FIELDS = set(["name", "timestamp", "fields"])

def create_topic(path):
    replacesments = set([':', '/'])
    rpath = path
    for ch in replacesments:
        rpath = rpath.replace(ch, ".")
    return rpath

def form_encoding_path(encoding_path, levels):
    if not levels:
        return encoding_path
    if encoding_path[-1] == "/":
        encoding_path = encoding_path[:-1]
    return '/'.join([encoding_path, '/'.join(levels)])

def add_leaf(encoding_path, name):
    if encoding_path[-1] == "/":
        encoding_path = encoding_path[:-1]
    return '/'.join([encoding_path, name])

def combine_keys_content(keys, content):
    # keys are a dict, content is a dict -> list of dicts
    combined = []
    for content_metric  in content:
        metric = dict(keys)
        metric.update(content_metric)
        combined.append(metric)
    return combined

    #for comb in itertools.product(*content.values()):
    #    metric = dict(keys)
    #    for subhierarchy in comb:
    #        metric.update(subhierarchy)
    #    yield metric

    #combined = {}
    #combined.update(keys)
    #combined.update(content)
    #return combined


ONE_OF = set(
    [
        "bytesValue",
        "stringValue",
        "boolValue",
        "uint32Value",
        "uint64Value",
        "sint32Value",
        "sint64Value",
        "doubleValue",
        "floatValue",
    ]
)
# Not sure what to do with int64 values. We'll keep them int for now.
INTEGERS = set(["uint32Value", "uint64Value", "sint32Value", "sint64Value"])
FLOAT = set(["doubleValue", "floatValue"])


