'''
Includes classes and code to work with cisco gpbkv data.
Note that gpbvk data could also come in python dict or in json, it just means that 
it is encoded using the a schema from the TelemetryField msg of the cisco_telemetry.proto
'''
from typing import Dict, Sequence, Any
from exceptions import PmgrpcdException

class CiscoGPBException(PmgrpcdException):
    pass


class EmptyValue(CiscoGPBException):
    pass

class CiscoGPBWarning(CiscoGPBException):
    '''
    Special type of exception used to signal warnings.
    '''
    pass

class UnknownField(CiscoGPBWarning):
    '''
    Found a field without a name field.
    NX has some of these. Others should, in theory, not.
    '''
    pass


NON_VALUE_FIELDS = set(["name", "timestamp", "fields"])
ONE_OF = set(
    [
        "bytes_value",
        "string_value",
        "bool_value",
        "uint32_value",
        "uint64_value",
        "sint32_value",
        "sint64_value",
        "double_value",
        "float_value",
    ]
)
# Not sure what to do with int64 values. We'll keep them int for now.
INTEGERS = set(["uint32_value", "uint64_value", "sint32_value", "sint64_value"])
FLOAT = set(["double_value", "float_value"])
INTEGERS64 = set(["uint64_value", "sint64_value"])

class PivotingCiscoGPBKVDict:
    '''
    Class including methods to pivot the data from Cisco GBPKV when data is a python dictionary
    '''

    def __init__(self, casting=False, cast_int64to_int=True):
        self.casting = casting
        self.cast_int64to_int = cast_int64to_int

    def pivot_telemetry_fields(self, fields, warnings=None) -> Sequence[Dict[str, Any]]:
        if warnings is None:
            warnings = set()
        new_fields = []
        for field in fields:
            new_field = self.pivot_telemetry_field(field, warnings)
            new_fields.append(new_field)
        return new_fields

    def pivot_telemetry_field(self, fields, warnings=None) -> Sequence[Dict[str, Any]]:
        if warnings is None:
            warnings = set()
        pivoted_field = {}
        if "timestamp" in fields:
            pivoted_field["timestamp"] = fields["timestamp"]
        if "fields" not in fields or not fields["fields"]:
            return pivoted_field
        keys_content = self.convert_telemetryfield_to_dict(fields, warnings=warnings)
        pivoted_field["keys"] =  keys_content.get("keys", [])
        pivoted_field["content"] = keys_content.get("content", [])
        return pivoted_field

    @staticmethod
    def is_key_value(fields) -> bool:
        """
        Returns yes if the field is of type key_content
        """
        return len(fields) == 2 and set(
            [field.get("name", "") for field in fields]
        ) == set(["keys", "content"])

    def convert_telemetryfield_to_dict(self, telemetry_field, warnings=None):
        """
        We assume a telemetry_field to be a dict at this point.
        Pivots a telemetry field.
        """
        flatten_content = {}
        if warnings is None:
            warnings = set()
        for field in telemetry_field["fields"]:
            if "fields" in field and field["fields"]:
                name = field.get("name", None)
                if name is None:
                    name = "Unknown"
                    warnings.add(UnknownField(f"Found Unknown"))
                value = self.convert_telemetryfield_to_dict(field, warnings)
            else:
                try:
                    name, value = self.simplify_cisco_field(field)
                except EmptyValue as e:
                    warnings.add(e)
                    continue
            self.add_to_flatten(flatten_content, name, value)
        # this is to deal with NX fields wwithout name
        if len(flatten_content) == 1 and "Unknown" in flatten_content:
            return flatten_content["Unknown"]
        return flatten_content

    @staticmethod
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


    def simplify_cisco_field(self, field, encoding_path=None, levels=None, key_n=None):
        # find the name, this becomes more of a problem when the mapping is complicated
        name = None
        #if encoding_path in self.names_data:
        #    if key_n is not None:
        #        name = self.names_data[encoding_path]["names"][0][key_n]
        #    else:
        #        name = "_".join([*levels, field["name"]])

        if name is None:
            # problem, log
            name = field["name"]

        try:
            value = self.cast_value(field)
        except EmptyValue:
            raise

        return name, value

    def cast_value(self, field):
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

        if self.casting: 
            # try to cast value.
            casting = None
            if found_attr in INTEGERS:
                casting = int
                if found_attr in INTEGERS64 and not self.cast_int64to_int:
                    casting = str
            elif found_attr in FLOAT:
                casting = float
            elif found_attr == "bool_value":
                casting = bool

            try:
                value = casting(value)
            except:
                pass

        return value
