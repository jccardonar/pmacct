from pyang import plugin
from pyang import statements
from pyang.statements import mk_path_str
from pygtrie import CharTrie
from itertools import chain
from pyang.types import is_base_type
import json
import pickle
import hashlib


paths = {
    "Cisco-IOS-XR-qos-ma-oper:qos/nodes/node/policy-map/interface-table/interface/input/service-policy-names/service-policy-instance/statistics",
    "Cisco-IOS-XR-qos-ma-oper:qos/nodes/node/policy-map/interface-table/interface/member-interfaces/member-interface/input/service-policy-names/service-policy-instance/statistics",
    "Cisco-IOS-XR-qos-ma-oper:qos/nodes/node/policy-map/interface-table/interface/member-interfaces/member-interface/output/service-policy-names/service-policy-instance/statistics",
    "Cisco-IOS-XR-qos-ma-oper:qos/nodes/node/policy-map/interface-table/interface/output/service-policy-names/service-policy-instance/statistics",
}


P = CharTrie()
P.update((x, True) for x in paths)


def pyang_plugin_init():
    plugin.register_plugin(ImplySpecPlugin())


class ImplySpecPlugin(plugin.PyangPlugin):
    def __init__(self):
        plugin.PyangPlugin.__init__(self, "imply")

    def add_output_format(self, fmts):
        self.multiple_modules = True
        fmts["imply"] = self

    def emit(self, ctx, modules, fd):
        for module in modules:
            process_module(ctx, module)


def process_module(ctx, module):
    metrics = {}
    # module should not be a list, so we dont need to process the metrics
    contents = []
    process_node(ctx, module, [], contents, metrics)
    if contents:
        raise Exception("Not prepared to deal with data without a dimension")
    # delete the nodes
    for node in list(metrics):
        path = get_node_path(node)

        if not P.longest_prefix(path).key:
            continue
        # create the json spec for this nodde
        nodes_to_names = get_names(node, metrics[node])
        spec = build_spec(node, metrics[node], nodes_to_names)
        json_spec = json.dumps(spec)
        name_file = get_node_path(node).replace("/", "_").replace(":", "_")
        name_file = "/tmp/" + name_file + ".json"
        with open(name_file, 'w') as fh:
            json.dump(spec, fh, indent=4)
        keys_content_names = prepare_names_change(node, metrics[node], nodes_to_names)
        name_file = get_node_path(node).replace("/", "_").replace(":", "_")
        name_info = "/tmp/" + name_file + "_name" + ".pickle"
        info_for_encoding_path = {}
        info_for_encoding_path["path"] = get_node_path(node)
        info_for_encoding_path["names"] = keys_content_names
        with open(name_info, 'wb') as fh:
            pickle.dump(info_for_encoding_path, fh)

    return metrics

def prepare_names_change(main_node, key_content, nodes_to_names):
    keys = key_content[0]
    contents = key_content[1]
    key_names = [nodes_to_names[k] for k in keys]
    encoding = get_node_path(main_node)
    content_names = {}
    for cn in contents:
        # we should improve this 
        relative_path = get_node_path(cn).replace(encoding, "")
        content_names[relative_path] = nodes_to_names[cn]
    return [key_names, content_names]

    

def build_spec(main_node, key_content, nodes_to_names):
    keys = key_content[0]
    content = key_content[1]
    spec = {}
    # the next one is not real;y needed, it seems
    spec["type"] = "kafka"

    # dataSchema
    dataSchema = {}
    spec["dataSchema"] = dataSchema
    dataSchema["dataSource"] = create_dataSource(main_node)
    parser = {}
    dataSchema["parser"] = parser
    parser["type"] = "string"
    parseSpec = {}
    parser["parseSpec"] = parseSpec
    parseSpec["timestampSpec"] = {"format": "millis", "column": "collection_start"}
    # add the standard dimensions
    dimensionsSpec = {}
    parseSpec["dimensionsSpec"] = dimensionsSpec
    dimensions = ["node_id"]
    parseSpec["format"] = "json"
    for node in chain.from_iterable([keys, content]):
        name = nodes_to_names[node]
        base_type = get_base_type(node)
        dimension_description = None
        if "int" in base_type.arg:
            dimension_description = {"name": name, "type": "long"}
        elif "decimal" in base_type.arg:
            dimension_description = {"name": name, "type": "double"}
        else:
            dimension_description = name
        dimensions.append(dimension_description)
    dimensionsSpec["dimensions"] = dimensions


    spec["ioConfig"] = {
        "type": "kafka",
        "topic": create_topic(main_node),
        "taskCount": 1,
        "consumerProperties": {
            "bootstrap.servers": "gtat-stage-msg01.ip.gin.ntt.net:9092,gtat-stage-msg02.ip.gin.ntt.net:9092"
        },
    }

    dataSchema["granularitySpec"] = {"rollup": False}

    spec["tuningConfig"] = {'type': 'kafka',
             'indexSpec': {'longEncoding': 'auto', 'bitmap': {'type': 'roaring'}}}

    return spec

def create_topic(node):
    path = get_node_path(node)
    replacesments = set([':', '/'])
    rpath = path
    for ch in replacesments:
        rpath = rpath.replace(ch, ".")
    return rpath

def create_dataSource(node):
    #path = get_node_path(node)
    #keys = [k.arg for k in get_keys(node)]
    #module = node.i_module.arg
    #return "_".join([module] + keys)
    #return create_topic(node)
    path = get_node_path(node)
    hash_p = hashlib.sha1(path.encode()).hexdigest()
    end = ".".join(path.split("/")[-2:])
    module = node.i_module.arg
    ds = module + ":" + hash_p + end
    return ds




def get_names(main_node, key_content):
    """
    Content can be in a different level than the children of the content.
    """
    names_to_nodes = {}
    # get the keys of the node (as nodes)
    keys = key_content[0]
    content = key_content[1]

    #for node in chain.from_iterable([keys, content]):
    #    name = node.arg
    #    names_to_nodes.setdefault(name, []).append(node)
    for node in keys:
        name = node.arg
        names_to_nodes.setdefault(name, []).append(node)
    main_path = get_node_path(main_node)
    for node in content:
        path = get_node_path(node)
        if main_path not in path:
            raise Exception("Cotent path not in main node")
        name = path.replace(main_path, "")
        if name[0] == "/":
            name = name[1:]
        name = name.replace("/", "_")
        names_to_nodes.setdefault(name, []).append(node)

    nodes_to_names = {}
    for name, nodes in names_to_nodes.items():
        if len(nodes) > 1:
            refined_names = find_name_for_samename_nodes(nodes)
            nodes_to_names.update(refined_names)
        else:
            nodes_to_names[nodes[0]] = name
    return nodes_to_names


def largest_common_substring(node, state=None):
    if state is None:
        state = []
    if len(node.children) > 1:
        return "".join(state)
    if node.children:
        for char, cn in node.children.items():
            state.append(char)
            return largest_common_substring(cn, state)
    return "".join(state)


def find_name_for_samename_nodes(nodes):
    """
    arg of the nodes must be similar
    """
    # order is important, for the first we'll leave the name, for the rest we'll add the previous name. If the names are still equal we add numbers in order.
    names_to_node = {}
    names_to_node[nodes[0]] = nodes[0].arg
    left_nodes = nodes[1:]
    proposed_names = {}
    for node in left_nodes:
        name = "_".join([node.parent.arg, node.arg])
        proposed_names.setdefault(name, []).append(node)
    for name, still_annouing_nodes in proposed_names.items():
        if len(still_annouing_nodes) > 1:
            i = CharTrie()
            for n, node in enumerate(still_annouing_nodes):
                # n_name = node.parent.parent.arg + "/" + name + str(n)
                n_name = get_node_path(node)
                i[n_name] = n_name.replace("/", "_")
                # names_to_node[node] = n_name
            largest_path = largest_common_substring(i._root)
            for n, node in enumerate(still_annouing_nodes):
                n_name = get_node_path(node)
                not_common = n_name.replace(largest_path, "").replace("/", "_")
                if not_common in names_to_node:
                    breakpoint()
                names_to_node[node] = not_common
        else:
            names_to_node[still_annouing_nodes[0]] = name
    return names_to_node


def get_node_path(node):
    if node.keyword == "module":
        return ""
    path = mk_path_str(node, prefix_onchange=True, prefix_to_module=True)
    if path[0] == "/":
        path = path[1:]
    return path


def process_node(ctx, node, keys, contents, metrics_data):
    # get the children nodes
    try:
        chs = [
            ch
            for ch in node.i_children
            if ch.keyword in statements.data_definition_keywords
        ]
    except:
        chs = []

    for ch in chs:
        path = get_node_path(ch)
        # if this is an encoding path, then we need to gather belo it
        if path in paths:
            new_content = []
            process_node(ctx, ch, keys, new_content, metrics_data)
            add_metric(ch, keys, new_content, metrics_data)
        # if this node is a list, restart the process
        elif ch.keyword == "list":
            if not ch.i_key:
                # contents.append(ch)
                process_node(ctx, ch, keys, contents, metrics_data)
                continue
            new_keys = list(keys)
            for key in ch.i_key:
                new_keys.append(key)
            new_content = []
            process_node(ctx, ch, new_keys, new_content, metrics_data)
            add_metric(ch, new_keys, new_content, metrics_data)
        else:
            process_node(ctx, ch, keys, contents, metrics_data)

    if node.keyword in set(["leaf", "leaf-list"]):
        contents.append(node)

    return metrics_data


def add_metric(node, keys, content, metrics_data):
    if node in metrics_data:
        raise Exception("repeated node")
    metrics_data[node] = (keys, content)


def get_keys(node, keys=None):
    if keys is None:
        keys = []
    if node.parent:
        get_keys(node.parent, keys)
    if node.keyword == "list":
        keys.extend(node.i_key)
    return keys


def get_base_type(stm):
    stmt_type = stm.search_one("type")
    if not stmt_type:
        breakpoint()
        raise Exception("Stm {} does not have a type".format(stm))
    if is_base_type(stmt_type.arg):
        return stmt_type
    stmt_typedefintion = stmt_type.i_typedef
    if not stmt_typedefintion:
        breakpoint()
        raise Exception("Stm {} does not have a typedef".format(stm))
    return get_base_type(stmt_typedefintion)
