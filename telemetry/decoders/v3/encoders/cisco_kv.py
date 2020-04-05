"""
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

We dont do joins on difffernet metrics.
But we do split (for later joints to solve them).

Internala packerts are just a dict (values can be complex puytho nobjects). Normally it would be content + metaata. An operation operates on the packet, but normally it would onl ydo it on content, or content + keys.

Outputs: kafka, files, console, zeromq (simple).
For kafka, we try to provide some control in how to select the topic of a value using metadata.
At atny point, we have metadata + payload. Metradata includes device, timestamp, path.

We do provide some fucntions for encoding conversion and data changes.

We can convert from  proto to json.
For kv (the more complex of all), we have our own SIMPLE json converter (not self described but proper). Keys and content are alwyas present. Keys are key/value but since names can be repeated, they become a tuple.

For turn json into dicts, and provide some tools for modification:

    Mitigations functsion: you just import your own function
    Key addition: you add your own keys.
    Name changes
    Filter (do not send more than over this path), do not include this type of data,

    Now, we do count with a flatten of json (it is actualy of s dict of key)
    If there is a list, we split on every element using the same keys.
    for mere containers, we bring them down using the name1_name2, or if given, we ignore the first part (careful with name collision).
    For name collision, we complain and add a _number. We complain using metrics.

    We allow to just leave numeric values here.

We ignore the timestamp of deeper hierarchies in ciscokv. 

We can export raw (together  with some metadata)
We can export in json (how to obtain it depends on each encoding)
We can export in json compress (which is just the json but compressed after)
We can try to flatten data
"""

from .base import BaseEncoding, BaseEncodingException, InternalMetric
import sys

# TODO: Fix this relative import
#sys.path.append("..")
#from cisco_pmgrpcd import process_cisco_kv

NON_VALUE_FIELDS = set(["name", "timestamp", "fields"])

class CiscoGPBException(BaseEncodingException):
    pass

class EmptyValue(CiscoGPBException):
    pass

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

# TODO this is repeated from code within, but it was creating cycles and importing was ugly. Fix.
def process_cisco_kv(new_msg):
    """
    Processes a msg using gpb-kv
    """
    telemetry_msg = cisco_telemetry_pb2.Telemetry()
    telemetry_msg.ParseFromString(new_msg.data)
    #jsonStrTelemetry = MessageToJson(telemetry_msg)
    #grpc_message = json.loads(jsonStrTelemetry)
    grpc_message = MessageToDict(telemetry_msg)
    return grpc_message

class CiscoKVFlatten(BaseEncoding):
    p_key = "encodingPath"

    @classmethod
    def build_from_grpc_msg(cls, msg, *args, **kargs):
        data = process_cisco_kv(msg)
        if cls.content_key not in data:
            data[cls.content_key] = {}
            if "dataGpbkv" in data and data["dataGpbkv"]:
                data[cls.content_key] = data["dataGpbkv"]
            data.pop("dataGpbkv", None)
        return cls(data, *args, **kargs)

    @classmethod
    def build_from_dcit(cls, content):
        data = content
        if cls.content_key not in data:
            data[cls.content_key] = {}
            if "dataGpbkv" in data and data["dataGpbkv"]:
                data[cls.content_key] = data["dataGpbkv"]
            data.pop("dataGpbkv", None)
        if cls.p_key not in data:
            data[cls.p_key] = data["encoding_path"]
        return cls(data)


    def __init__(self, data, names_data=None, extra_keys=None):
        if names_data is None:
            names_data = {}
        if extra_keys is None:
            extra_keys = {}
        self.names_data = names_data
        self.extra_keys = extra_keys
        return super().__init__(data)

    def get_internal(self):
        for sample in self.content:
            keys, content = self.convert_ciscokv_to_dict(sample)
            data = self.data.copy()
            data[self.content_key] = content
            data[self.keys_key] = keys
            yield InternalMetric(data)

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

    def convert_telemetryfield_to_dict(self, telemetry_field):
        """
        A telemetryfield is a msg, that is, a dict.
        """
        flatten_content = {}
        for field in telemetry_field["fields"]:
            if "fields" in field and field["fields"]:
                name = field.get("name", None)
                if name is None:
                    name = "Unknown"
                value = self.convert_telemetryfield_to_dict(field)
            else:
                try:
                    name, value = self.simplify_cisco_field(field)
                except EmptyValue as e:
                    # TODO: Should we log this?
                    continue
            self.add_to_flatten(flatten_content, name, value)
        return flatten_content

    def convert_ciscokv_to_dict(self, fields):
        keys_content = self.convert_telemetryfield_to_dict(fields)
        keys = keys_content.get("keys", [])
        content = keys_content.get("content", [])
        return keys, content

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

        try:
            value = self.cast_value(field)
        except EmptyValue:
            raise

        return name, value

    @staticmethod
    def cast_value(field):
        """
        Obtains a value from a TelemetryField. 
        """
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
            raise EmptyValue("We could not find a way of simplifying {}".format(field))

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
