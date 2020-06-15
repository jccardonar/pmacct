from pathlib import Path
from typing import Union


def data_folder(base=None):
    if base is None:
        base = __file__
    base_file = Path(base)
    return base_file.parents[0] / "data"

class TelemetryTests(Exception):
    pass

class FileDoesNotExist(TelemetryTests):
    pass

FileLocation = Union[Path, str]

def process_file_name(file_name: FileLocation) -> Path:
    '''
    Shortcut for validating a file and converting it to Path
    '''
    file_name = Path(file_name)
    if not file_name.exists():
        raise FileDoesNotExist(f"File {file_name} does not exist")
    return file_name

def load_dump_file(file_name: FileLocation) -> str:
    file_name = process_file_name(file_name)
    with open(file_name, 'r') as fh:
        return fh.read()

def load_dump_line(file_name: Union[Path, str], line_number: int):
    '''
    Returns a single line of a line. Line start with 1.
    Fails if line does not exist.
    '''
    content = load_dump_file(file_name).split("\n")
    if len(content) < line_number:
        raise TelemetryTests(f"Line {line_number} is not valid. {file_name} has {len(content)} lines")
    return content[line_number-1]


BASIC_PROPERTIES = ["collection_timestamp", "collection_end_time", "collection_start_time", "msg_timestamp", "collection_id", "path", "node_id", "subscription_id"]

def check_metric_properties(metric):
    '''
    Test the basic metric properties.
    No matter the result, it should not raise an exception
    '''
    failing_attr = []
    for attr in BASIC_PROPERTIES:
        try:
            _ = getattr(metric, attr)
        except:
            failing_attr.append(attr)
    return failing_attr




