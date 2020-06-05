'''
Includes code to handle the nx api.
'''
from exceptions import PmgrpcdException

class PivotingNXApiDict:
    '''
    Transforms the NX API model into a more plain relationship model.
    The NX API especifies "children" elements into the "children" attribute.
    Here we remove the children element and other intermediary 
    that are not necessary
    '''

    ELEMENT_KEYS = set(["attributes", "children"])

    @classmethod
    def build_from_internal(cls, internal):
        data = internal.data.copy()
        return cls(data)

    def detect_element(self, element):
        if (
            element
            and isinstance(element, dict)
            and set(element.keys()).issubset(self.ELEMENT_KEYS)
        ):
            return True
        return False

    @staticmethod
    def find_key(flatten_content, key):
        if key not in flatten_content:
            return key
        for n in range(0, 20):
            nkey = "_".join([key, str(n)])
            if nkey not in flatten_content:
                return nkey
        raise Exception("We could not find key")

    def convert_nx_element(self, element, warnings=None):
        if warnings is None:
            warnings = set()
        new_element = {}
        # find children first
        keys_list = set()
        children = element.get("children", [])
        if not isinstance(children, list):
            children = [children]
        for children_object  in children:
            # not sure what to do with childrens that have two elements.
            for old_key, v in children_object.items():
                rn, nv = self.pivot_nx_api(v, warnings)
                if rn is None:
                    # complain
                    k = old_key
                else:
                    if "[" in rn:
                        k = rn.split("[")[0]
                    else:
                        k = rn
                if k in new_element and k not in keys_list:
                    old_element = new_element[k]
                    new_element[k] = [old_element]
                    keys_list.add(k)
                if k in new_element:
                    new_element[k].append(nv)
                else:
                    new_element[k] = nv

        # just get the attruibutes
        for k, v in element.get("attributes", {}).items():
            nk = self.find_key(new_element, k)
            new_element[nk] = v
        rn = element.get("attributes", {}).get("rn", None)
        return rn, new_element

    def pivot_nx_api(self, content, warnings=None):
        if warnings is None:
            warnings = set()
        if isinstance(content, list):
            new_content = []
            for element in content:
                new_element = self.convert_nx_api(element)
                new_content.append(new_element)
            return new_content
        if isinstance(content, dict):
            if self.detect_element(content):
                rn, new_content = self.convert_nx_element(content, warnings)
                return rn, new_content
            new_content = {}
            for k, element in content.items():
                if self.detect_element(element):
                    rn, new_element = self.convert_nx_element(element, warnings)
                else:
                    raise Exception("Wrong point")
                new_content[k] = new_element
            return new_content
        raise Exception("Type not identified")

