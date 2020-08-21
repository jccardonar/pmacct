from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from exceptions import PmgrpcdException
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Type,
    Generator,
    TypeVar,
    Any,
    Sequence
)
from metric_types.base_types import SubTreeData


class TransformationException(PmgrpcdException):
    def __init__(self, text, fields: Optional[Dict[str, str]] = None, *args, **kargs):
        if fields is None:
            fields = {}
        self.fields = fields
        super().__init__(text)


# from https://stackoverflow.com/questions/34073370/best-way-to-receive-the-return-value-from-a-python-generator
class GeneratorReturnAfterFor:
    def __init__(self, gen):
        self.gen = gen

    def __iter__(self):
        self.value = yield from self.gen


MetricTypeGeneric = TypeVar("MetricTypeGeneric", bound="SubTreeData")


class InvalidTransformationConstruction(TransformationException):
    pass

T = TypeVar("T", bound="TransformationBase")


class TransformationBase(ABC):

    TRANSFORMATIONS: Dict[str, Type] = {}
    _dict_key:Optional[str] = None

    @classmethod
    def dict_key(cls):
        '''
        Returns _dict_key if it is none, if not it returns the class name.
        '''
        if cls._dict_key is None:
            return cls.__name__
        return cls._dict_key

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.dict_key() is None:
            return
        if cls.dict_key() in cls.TRANSFORMATIONS:
            raise InvalidTransformationConstruction(f"cls.dict_key {cls.dict_key()} for {cls} is already used by {cls.TRANSFORMATIONS[cls.dict_key()]}")
        cls.TRANSFORMATIONS[cls.dict_key()] = cls

    @classmethod
    def from_dict(cls: T, config: Dict[str, Any]) -> T:
        '''
        Constructs a class based on a dict. See to_dict.
        '''
        raise NotImplementedError


    def to_dict(self) -> Dict[str, Any]:
        '''
        Returns the configuration of the transformation to a dict. Not all transformation would have it.
        If they do THEY MUST define an unique cls.dict_key, also if they define a cls.dict_key they should
        support from_dict and to_dict (which unfortunatly, I dont know how to simple test)
        '''
        raise NotImplementedError


    @abstractmethod
    def transform(
        self, metric: MetricTypeGeneric, warnings=None
    ) -> Generator[MetricTypeGeneric, None, None]:
        """
        This is the core of the transformation functions.
        The function takes a metric and generates others.
        Transformations that return only one value are converters, we define them in a 
        different class.
        """

    def __str__(self):
        """
        We print a transformrmation just with its class
        """
        return str(self.__class__.__name__)

    def transform_list(self, generator: Iterable[MetricTypeGeneric], warnings=None):
        """
        Takes a generator of metrics and transforms. 
        Since the split transformations return, the auxiliary Generator keeps 
        the return value so we can return it.
        to do  "= yield from"
        """
        if warnings is None:
            warnings = []
        generagtor_with_return = GeneratorReturnAfterFor(generator)
        for metric in generagtor_with_return:
            yield from self.transform(metric, warnings)
        return generagtor_with_return.value

class InvalidTransformationdDump(TransformationException):
    pass


KEY_TRANSFORMATION_NAME = "transformation"
KEY_CONFIG = "config"
REQUIRED_KEYS = set([KEY_CONFIG])

def load_transformation(config: Dict[str, Any]) -> TransformationBase:
    '''
    Builds a transformation using the provided from_dict function.
    :param config: A dictionary with the instructions to build the transformation.
        It should contain an key called "transformation" with the name of the transformation,
        and another called "config" with the dict to build the transformation
    :raises InvalidTransformationConstruction: If config is not correctly built, if the transformation does not exist (maybe it is not loaded).
    '''
    missing = REQUIRED_KEYS - set(config) 
    if missing:
        raise InvalidTransformationConstruction(f"Missing {missing} keys in the config dictionary")
    tranformation_id = config[KEY_TRANSFORMATION_NAME]
    if tranformation_id not in TransformationBase.TRANSFORMATIONS:
        raise InvalidTransformationConstruction(f"{tranformation_id} not found in classes. Make sure the class defines a proper dict_key")
    tranformation_class = TransformationBase.TRANSFORMATIONS[tranformation_id]
    try:
        transformation = tranformation_class.from_dict(config[KEY_CONFIG])
    except Exception as e:
        raise InvalidTransformationConstruction("Error constructing tranfromation") from e
    return transformation


def dump_transformation(transformation: TransformationBase) -> Dict[str, Any]:
    '''
    Dumps a transforamtion to a dict, if possible. Uses the to_dict function and builds the dict with the correct keys.
    :param transformation: A transformation. Its class must support the to_dict function and have a its dict_key defined.
    '''
    if transformation.__class__.dict_key() not in TransformationBase.TRANSFORMATIONS:
        raise InvalidTransformationdDump(f"{transformation.__class__} does not have a proper dict_key. Define it if you want to dump it")

    try:
        config = transformation.to_dict()
    except Exception as e:
        raise InvalidTransformationdDump("We could not dump transformation to dict") from e

    return {KEY_TRANSFORMATION_NAME: transformation.__class__.dict_key(), KEY_CONFIG: config}

def load_transformations(configs: Iterable[Dict[str, Any]]) -> Sequence[TransformationBase]:
    '''
    Loads multiple transformations 
    '''
    return [load_transformation(c) for c in configs]

def dump_transformations(tranformations: Iterable[TransformationBase]) -> Sequence[Dict[str, Any]]:
    '''
    Dumps multiple transformations.
    '''
    return [dump_transformation(t) for t in tranformations]


class MetricTransformationBase(TransformationBase):
    """
    A skeleton for a transformation that stores data per path.
    """

    def __init__(self, data_per_path=None):
        if data_per_path is None:
            data_per_path = {}
        self.data_per_path = data_per_path


class BaseConverter(TransformationBase):
    """
    A converter is a transformation that is only one to one
    It brings the convert
    """

    def convert(self, metric: MetricTypeGeneric, warnings=None) -> MetricTypeGeneric:
        transformation = list(self.transform(metric, warnings))
        if len(transformation) != 1:
            raise Exception("Transformation returns more than one lement")

        return transformation[0]


class SimpleConversion(BaseConverter):
    """
    Base for a transformation that basically "casts" one metric to another.
    This could be similar to using  object.__class__ = NewClass, 
    but it looks less hacky.
    Abstract handeling from https://stackoverflow.com/questions/45248243/most-pythonic-way-to-declare-an-abstract-class-property
    """

    ORIGINAL_CLASS: Optional[Type] = None
    RESULTING_CLASS: Optional[Type] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.ORIGINAL_CLASS is None:
            raise NotImplementedError("Please define the original class")
        if cls.RESULTING_CLASS is None:
            raise NotImplementedError("Please define the resulting class")

    @classmethod
    def transform(cls, metric, warnings=None):
        data = metric.data.copy()
        # Have empty content, if not available.
        if cls.RESULTING_CLASS.content_key not in data:
            data[cls.RESULTING_CLASS.content_key] = []
        yield cls.RESULTING_CLASS(data)


# Pipe classes
# Helper classes to allow for a more fluid transformation of metrics


@dataclass(frozen=True)
class MetricState:
    """
    Allows for a fluid conversion of elements.
    Transformations should use TransformationState
    """

    _metric: SubTreeData
    warnings: List[Exception] = field(default_factory=lambda: [])
    error: Optional[TransformationException] = None

    @property
    def is_error(self):
        return self.error is not None

    @property
    def metric(self) -> Optional[SubTreeData]:
        if self.is_error:
            return None
        return self._metric

    @property
    def error_metric(self) -> Optional[SubTreeData]:
        if not self.is_error:
            return None
        return self._metric

    def convert(self, converter: BaseConverter) -> "MetricState":
        """
        Allows to pipe converters keeping the warnings:
        cs.convert(tr).convert(tr).convert(tr)
        """
        if self.is_error:
            return self
        warnings_copy = list(self.warnings)
        try:
            new_metric = converter.convert(self.metric, warnings_copy)
        except TransformationException as e:
            # there is a failure, therefore, return a "Nothing"
            e.fields["tranformation"] = str(converter)
            return MetricState(self.metric, warnings_copy, e)
        # We fail for non TransformationException failures.
        return MetricState(new_metric, warnings_copy)

    def convert_raiser_error(self, converter):
        new_state = self.convert(converter)
        if new_state.is_error:
            raise new_state.error
        return new_state

    def pipeline(self, converters: List[BaseConverter], fail_if_exception=False):
        """
        Passes the metric state through a series of converters
        """
        current_metric_state = self
        for convertr in converters:
            current_metric_state = current_metric_state.convert(convertr)
        if fail_if_exception and current_metric_state.is_error:
            raise current_metric_state.error
        return current_metric_state


@dataclass(frozen=True)
class TransformationState:
    metrics_state: List[MetricState]
    warnings: List[Exception] = field(default_factory=lambda: [])

    @classmethod
    def from_metric(cls, metric):
        """
        Wrap a single metric
        """
        return cls([MetricState(metric)])

    def transform_gen(
        self, transformer: TransformationBase, warnings
    ) -> Generator["MetricState", None, None]:
        """
        Allows for pipes of transformations
        """
        for metric_state in self.metrics_state:
            # deal with a previous error
            if metric_state.is_error:
                yield metric_state
                continue
            try:
                for new_metric in transformer.transform(metric_state.metric, warnings):
                    yield MetricState(new_metric)
            except TransformationException as e:
                e.fields["tranformation"] = str(transformer)
                yield MetricState(metric_state.metric, error=e)
            # We fail for non TransformationException failures.

    def transform(self, transformer):
        new_warnings = list(self.warnings)
        new_metrics_state = list(self.transform_gen(transformer, new_warnings))
        return TransformationState(new_metrics_state, new_warnings)

    def pipeline(self, transformers: List[TransformationBase]) -> "TransformationState":
        current_state = self
        for transf in transformers:
            current_state = current_state.transform(transf)
        return current_state

    @classmethod
    def apply_transformation(cls, metric: SubTreeData, pipeline, *, on_error, on_warnings) -> Sequence[SubTreeData]:
        '''
        Applies a pipeline to a metric, and returns directly the list of metrics. Does the heavy lifting of "lifting" metrics.
        Function with side effects, since it might call on_error  and on_warnings
        Receives a on_error callback to apply to all metric errors.
        Receives n on_warnings callback to apply to the list of warnings.
        '''
        resulting_state = cls.from_metric(metric).pipeline(pipeline)
        if resulting_state.warnings and on_warnings:
            on_warnings(resulting_state.warnings)
        metrics = []
        for metric_state in resulting_state.metrics_state:
            if metric_state.is_error:
                if on_error:
                    on_error(metric_state.error)
                continue
            metrics.append(metric_state.metric)
        return metrics

