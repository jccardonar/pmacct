"""
Utils for the gnmi package
"""
# from pyang import xpath_parser
from protos import gnmi_pb2
from typing import Tuple, Sequence

AXIS_SUPPORTED = set(["child", "descendant-or-self"])
SUPPORTED_PREDICATES = set(["relative", "path_expr"])

# From https://stackoverflow.com/questions/18092354/python-split-string-without-splitting-escaped-character
def split_unescape(s, delim, escape='\\', unescape=True):
    """
    From https://stackoverflow.com/questions/18092354/python-split-string-without-splitting-escaped-character
    >>> split_unescape('foo,bar', ',')
    ['foo', 'bar']
    >>> split_unescape('foo$,bar', ',', '$')
    ['foo,bar']
    >>> split_unescape('foo$$,bar', ',', '$', unescape=True)
    ['foo$', 'bar']
    >>> split_unescape('foo$$,bar', ',', '$', unescape=False)
    ['foo$$', 'bar']
    >>> split_unescape('foo$', ',', '$', unescape=True)
    ['foo$']
    """
    ret = []
    current = []
    itr = iter(s)
    for ch in itr:
        if ch == escape:
            try:
                # skip the next character; it has been escaped!
                if not unescape:
                    current.append(escape)
                current.append(next(itr))
            except StopIteration:
                if unescape:
                    current.append(escape)
        elif ch == delim:
            # split! (add current to the list and reset it)
            ret.append(''.join(current))
            current = []
        else:
            current.append(ch)
    ret.append(''.join(current))
    return ret

def parse_gnmi_metadata(text) -> Sequence[Tuple[str, str]]:
    r'''
    Transform a string into two value tuples. \ for escaping other commas or =.
    >>> parse_gnmi_metadata('key=1,key2=2')
    (('key', '1'), ('key2', '2'))
    >>> parse_gnmi_metadata('key=1\,,key2=\=2')
    (('key', '1,'), ('key2', '=2'))
    >>> parse_gnmi_metadata('')
    ()
    '''
    if not text:
        return ()
    # deal with escaping.
    element_splitter = ","
    kv_splitter = "="
    elements = split_unescape(text, element_splitter)
    results = []
    for elem in elements:
        if kv_splitter not in elem:
            raise Exception(f"Element in gnmi textdata does not contain {kv_splitter}")
        splited = elem.split(kv_splitter, 1)
        results.append(tuple(splited))
    return tuple(results)

class PathNotSupported(Exception):
    pass


ESCAPED_CHRS = set(["]", "\\"])
E = "\\"


def split_gnmi(path, sch):
    """
    Escaping in gnmi paths is only inside the predicate
    and for \\ and ], we leverage this for this psemi-general function.
    """
    current_step = []
    in_predicate = False
    for n, ch in enumerate(path):
        # we aadd it as long as it is not a breakpoint
        add = True
        if ch == "]":
            if in_predicate and path[n - 1] != E:
                in_predicate = False

        if ch == sch and not in_predicate:
            yield "".join(current_step)
            current_step = []
            # this is the only breaking point, we must continue processing in case this
            # is also [
            add = False

        if ch == "[":
            in_predicate = True

        if add:
            current_step.append(ch)

    if current_step:
        yield "".join(current_step)


def split_path(path):
    if path[0] == "/":
        path = path[1:]
    yield from split_gnmi(path, sch="/")


def split_predicates(predicates):
    if not predicates:
        return
    if not (predicates[0] != "[" or predicates[-1] == "]"):
        raise PathNotSupported(
            f"predicates must not start with [ and end with  ]. Got {predicates}"
        )
    yield from split_gnmi(predicates, sch="[")


def parse_step(step):
    name, *predicates = split_predicates(step)
    if E in name:
        raise PathNotSupported(f"{name} includes the escape ch")
    constraints = {}
    for p in predicates:
        if p[-1] != "]":
            raise PathNotSupported(f"{p} last ch is not ].")
        p = p[:-1]
        key, value = p.split("=")
        if E in key:
            raise PathNotSupported(f"{key} includes the escape ch")
        rvalue = []
        escaped = False
        for ch in value:
            if escaped:
                if ch not in ESCAPED_CHRS:
                    raise PathNotSupported(
                        f"{value} is escaping a ch different from {ESCAPED_CHRS}"
                    )
                escaped = False
            elif ch == E:
                # The elif is very important here!
                escaped = True
                continue
            rvalue.append(ch)
        if escaped:
            raise PathNotSupported(f"{value} last character escapes")

        rvalue = "".join(rvalue)
        if key in constraints:
            raise PathNotSupported(
                f"{key} is included in more than in one predicate. Not supported"
            )
        constraints[key] = rvalue
    return name, constraints


def simple_gnmi_string_parser(path):
    """
    https://github.com/openconfig/reference/blob/master/rpc/gnmi/gnmi-path-conventions.md
    Using also examples from 
    https://github.com/google/gnxi/blob/master/utils/xpath/xpath.go#L255
    We are not going to differentiate between prefix and path for now.
    TODO: pyang has a parser for xpaths. I would prefer to use that one since it would be better maintained.
    """
    # First, let us split the paths into steps.
    # / within predicates
    elements = []
    for step in split_path(path):
        name, constraints = parse_step(step)
        if not name:
            raise PathNotSupported(f"Name  must not be empty. Empty name in {step}")
        elements.append(gnmi_pb2.PathElem(name=name, key=constraints))
    return gnmi_pb2.Path(elem=elements)


# def xpath_to_pathgnmi(path):
#    '''
#    I wanted to have a xpath to gnmi string and back, this is currently not used. I might finish it later.
#    '''
#    # all paths are considered from root, ignore first step delimiter if present.
#    if path[0] == STEP_SPLITTER:
#        path = path[1:]
#    parsed_path = xpath_parser(path)
#    if parsed_path[1] != "relative":
#        raise PathNotSupported("Location path can only be relative after removing root")
#
#    so_far ='/'
#    for step in parsed_path[0]:
#        this_element = None
#        if step[0] != "step":
#            raise PathNotSupported(f"We only support step elements, got {step}.  Currently in {so_far}")
#        _, axis, nodetest, preds = step
#        if axis not in AXIS_SUPPORTED:
#            raise PathNotSupported(f"We only support axis {AXIS_SUPPORTED}, got {axis}. Currently in {so_far}")
#
#        if axis == "child":
#            if nodetest[0] != "name":
#                raise PathNotSupported(f"We only support Name nodetest, got {nodetest[0]}. Currently in {so_far}")
#            name = nodetest[2]
#            predicates = {}
#            for pred in preds:
#
#                if not (pred[0] == "comp" and pred[1] == "="):
#                    raise PathNotSupported(f"We only support predicates with comparison equal operations, got  {pred}. Currently in {so_far}")
#
#                key = None
#                value = None
#                for op in pred[2:]:
#                    # I hope the parser never places no more than 2 ops here, not testing.
#                    if op[0] not in SUPPORTED_PREDICATES:
#                        raise PathNotSupported(f"We only support predicate element  with {SUPPORTED_PREDICATES}, got  {op}. Currently in {so_far}")
#                    if op[0] == "relative":
#                        # this is the key, or the value if it is a wildcard
#                        if not (op[1][0] == "step" and  op[1][1] == "child" and op[1][2][0] == "name" and not p[1][3]):
#                            raise PathNotSupported(f"We only support predicate keys that are simple childs, got  {op}. Wildcards in predicate also not supported. Currently in {so_far}")
#                        key = op[1][2][2]
#                    elif op[0] == "path_expr":
#                        # this is the value
#                        value = op[1][0][1]
#                    else:
#                        raise PathNotSupported(f"We only support predicate element  with {SUPPORTED_PREDICATES}, got  {op}. Broken if statements. Currently in {so_far}")
#                if key is None or value is None:
#                    raise PathNotSupported(f"No key defined in predicate {pred}, remember that strings must be quoted and node names not.")
#                predicates[key] = value
#
#            gnmi_pb2.PathElem(name=name, key=predicates)
#
#        elif axis == "descendant-or-self":
#            gnmi_pb2.PathElem(name=name, key=predicates)
#
#        else:
#            raise PathNotSupported(f"We only support axis {AXIS_SUPPORTED}, got {axis}. Currently in {so_far}")
#
