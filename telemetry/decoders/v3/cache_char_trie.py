"""
A trie for chars based on CharTrie that speeds up access by caching.
We need tries, but they are built and then become read-only. We thus spped up reading by caching in a dict.
"""
from pygtrie import CharTrie
from typing import Any, Dict, Optional, Sequence, Union, Iterable

# TODO: First try to use caching in a trie, since accessing data was the more consuming task
# More work might be needed
# key here was to cache __contains__
class CacheCharTrie(CharTrie):  # pylint: disable=too-many-ancestors
    """
    Functions as a CharTrie but with caching. 
    It should be used  only if reading happens after building the Trie.
    >>> trie = CacheCharTrie()
    >>> trie['module:thi/is/a/path'] = True
    >>> bool(trie.has_node('module:thi'))
    True
    >>> bool(trie.has_node('module:this'))
    False
    >>> 'module:this' in trie
    False
    >>> 'module:thi/is/a/path' in trie
    True
    """

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        self.cache_node = {}
        self.cache = {}
        self.cache_item = {}

    def _get_node(self, key, *args, **kargs):
        if key in self.cache:
            return self.cache[key]
        value = super()._get_node(key, *args, **kargs)
        self.cache[key] = value
        return value

    def has_node(self, key):
        if key in self.cache_node:
            return self.cache_node[key]
        value = super().has_node(key)
        self.cache_node[key] = value
        return value

    def __contains__(self, key):
        if key in self.cache_item:
            return self.cache_item[key]
        value = super().__contains__(key)
        self.cache_item[key] = value
        return value


def get_trie(config, key):
    paths = config[key]
    if isinstance(paths, list):
        extra_keys_trie = CacheCharTrie()
        extra_keys_trie.update({x: True for x in paths})
        return extra_keys_trie
    extra_keys_trie = CacheCharTrie()
    extra_keys_trie.update(paths)
    return extra_keys_trie


def get_trie_from_sequence(a_list: Iterable[str]) -> CacheCharTrie:
    extra_keys_trie = CacheCharTrie()
    extra_keys_trie.update({x: True for x in a_list})
    return extra_keys_trie


def get_trie_from_dict(a_dict: Dict) -> CacheCharTrie:
    extra_keys_trie = CacheCharTrie()
    extra_keys_trie.update(a_dict)
    return extra_keys_trie
