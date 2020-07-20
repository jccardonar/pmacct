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
)
from metric_types.base_types import SubTreeData


class TransformationException(PmgrpcdException):
    def __init__(self, text, fields: Optional[Dict[str, str]] = None, *args, **kargs):
        if fields is None:
            fields = {}
        self.fields = fields
        super().__init__(text)


class NotAConverstion(TransformationException):
    pass


# from https://stackoverflow.com/questions/34073370/best-way-to-receive-the-return-value-from-a-python-generator
class GeneratorReturnAfterFor:
    def __init__(self, gen):
        self.gen = gen

    def __iter__(self):
        self.value = yield from self.gen


MetricTypeGeneric = TypeVar("MetricTypeGeneric", bound="SubTreeData")


class TransformationBase(ABC):
    @abstractmethod
    def transform(
        self, metric: MetricTypeGeneric, warning=None
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


class MetricTransformationBase(TransformationBase):
    """
    A skeleton for a transformation that stores data per path.
    """

    def __init__(self, data_per_path):
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
                yield MetricState(new_metric, error=e)
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
