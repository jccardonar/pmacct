'''
The next classes is about mapping a key to a property. We could do that with
plenty of libraries or solutions (see
https://stackoverflow.com/questions/4984647/accessing-dict-keys-like-an-attribute),
but since we only take a subset of keys, it is read only, and we need to map
the properties to different keys, then I ended up doing my own.
'''
from typing import Dict, Any
from exceptions import MetricException


class KeyErrorMetric(MetricException):
    pass

class AttrNotFound(MetricException):
    pass

class InvalidClassConstruction(MetricException):
    pass

def dict_attribute(f):
    """
    Using the function name, it returns the value from the dict
    This is highly tighed to the DictSubTreeData class. 
    I do not use a metaclass type of trick for this to be able to document
    properties, but I might do it later
    """
    def new_function(self):
        attr_name = f.__name__
        return self.get_attr_from_dict(attr_name)

    new_function._dir_attr = True
    prop_obj = property(new_function)
    return prop_obj

class DictProxy:
    '''
    A class that provides methods for creating properties that access an
    internal dictionary to get its value.  This is done, however, using an
    extra level of indirection.  That we mean by this is that the mapping
    between properties and the internal dictionary is done through another
    internal data structure.

    The keys for the properties in the internal dictionary are looked using
    other class attributes. For this explanation, we  all the first value
    properties, and the second key properties. 
    
    We map value properties to their key attributes in the _attr_to_key.

    For example, we want a property "timestamp". To locate "timestamp"
    we use the key located in the "timestamp_key" property.

    The mapping between value and key properties is done in _attr_to_key, the internal dict is accesible through "data".

    The key in the internal dict can be anything. From "timestamp" to "collection_timestamp".

    The extra indirection level should allow subclasses to modify it easily, so if
    a property has another key, we just need to replace the right class property.
    
    This is a read only proxy.
    '''
    _attr_to_key: Dict[str, str] = {}

    def __init__(self, data: Dict[str, Any]):
        self.data = data


    def __init_subclass__(scls, *args, **kwargs):
        '''
        Here we try to impose correct subclasses, by making 
        sure the value and key properties are defined.
        '''
        super().__init_subclass__(**kwargs)
        # make sure the subclass kas the properties for all the keys it defines.
        scls_attr_to_key = scls._attr_to_key
        for val_prop, key_prop in scls_attr_to_key.items():
            key_in_dict = getattr(scls, key_prop, None)
            if key_in_dict is None:
                raise InvalidClassConstruction(f"{key_prop} is not defined in class {scls}")

            val_prop_obj = getattr(scls, val_prop, None)
            if val_prop_obj is None:
                raise InvalidClassConstruction(f"{val_prop} is not defined in class {scls}")

        # finally, make sure that any function created through dict_attribute has its correpondent entry in the dict
        for name, method in scls.__dict__.items():
            if isinstance(method, property):
                is_dict_prop = getattr(method.fget, "_dir_attr", False)
                if is_dict_prop:
                    if name not in scls_attr_to_key:
                        raise InvalidClassConstruction(f"{name} does not have an entry in the _attr_to_key dict")


    @classmethod
    def get_attr_key(cls, attr: str):
        """
        Returns the property name for the key of one 
        of the main attributes.
        Raises AttrNotFound if main attribute does not exist.
        """
        if attr not in cls._attr_to_key:
            raise AttrNotFound(f"Key for {attr} not found")
        attr_key = cls._attr_to_key[attr]
        return getattr(cls, attr_key)

    def get_attr_from_dict(self, attr):
        attr_key = self.get_attr_key(attr)
        return self.load_from_data(attr_key, attr)

    def load_from_data(self, key: str, name: str) -> Any:
        """
        Loads from the internal data.
        :param key: Key of the parameter in the internal dict
        :param name: Name of the parameter. Only used for debugging purposes.
        """
        if name is None:
            name = key
        if key not in self.data:
            raise KeyErrorMetric(f"Error getting {name}, {key} not present in data.")
        return self.data[key]
