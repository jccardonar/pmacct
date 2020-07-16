"""
Proto converts proto objects to python dicts using the proto specification for json.
The specs define that uint64 fields should translate to string. This is consistent to the IETF recommedation.
According to some sources (TODO: add them), it is because some json libraries do not support uint64, although the json specifications does not mention this.
We want to have uint64 be integers, so we modify the conversion to not use it. This is not exhaustedly tested, so treat it as experimental.
"""
from google.protobuf.json_format import _Printer, _INT64_TYPES


class NewPrinter(_Printer):
    def __init__(self, *args, uint64_to_int=False, **kargs):
        self.uint64_to_int = uint64_to_int
        super().__init__(*args, **kargs)

    def _FieldToJsonObject(self, field, value):
        if self.uint64_to_int and field.cpp_type in _INT64_TYPES:
            return int(value)
        return super()._FieldToJsonObject(field, value)


def MessageToDictUint64(
    message,
    including_default_value_fields=False,
    preserving_proto_field_name=False,
    use_integers_for_enums=False,
    descriptor_pool=None,
    float_precision=None,
    uint64_to_int=True,
):
    printer = NewPrinter(
        including_default_value_fields,
        preserving_proto_field_name,
        use_integers_for_enums,
        descriptor_pool,
        uint64_to_int=uint64_to_int,
        float_precision=float_precision,
    )
    return printer._MessageToJsonObject(message)


def MessageToDictWithOptions(msg, options=None):
    if options is None:
        options = {}
    uint64_to_int = not options.get("proto_uint64_to_str", False)
    use_integers_for_enums = not options.get("proto_enums_to_str", False)
    msg = MessageToDictUint64(
        msg,
        including_default_value_fields=True,
        preserving_proto_field_name=True,
        use_integers_for_enums=use_integers_for_enums,
        uint64_to_int=uint64_to_int,
    )
    return msg
