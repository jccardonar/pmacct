from abc import ABC, abstractmethod
from typing import Iterable, Sequence, Dict, Union, Any, Tuple, List, Optional
from dataclasses import dataclass, field
from exceptions import PmgrpcdException

class TransformationException(PmgrpcdException):
    def __init__(self, text, fields=None, *args, **kargs):
        if fields is None:
            fields = {}
        self.fields = {}
        super().__init__(text, fields, *args, **kargs)


# from https://stackoverflow.com/questions/34073370/best-way-to-receive-the-return-value-from-a-python-generator
class Generator:
    def __init__(self, gen):
        self.gen = gen

    def __iter__(self):
        self.value = yield from self.gen

class MetricTransformationBase(ABC):
    def __init__(self, data_per_path):
        self.data_per_path = data_per_path
        self._warning = None

    def set_warning_function(self, warning_function):
        self._warning = warning_function

    def warning(self, warning):
        """
        Warnings are msg per packet. They could be logged or used in metrics if needed.
        """
        if self._warning is None:
            return

        # should we catch a failure here, what to do?
        try:
            self._warning(warning)
        except:
            pass

    @abstractmethod
    def transform(self, metric, warnings: List[Exception]) -> Sequence["InternalMetric"]:
        pass

    def transform_list(self, generator, warnings=None):
        """
        Takes a generator of metrics and transforms. 
        Since the split transformations return, the auxiliary Generator keeps 
        the return value so we can return it.
        to do  "= yield from"
        """
        if warnings is None:
            warnings = []
        # TODO: modify the namme Generator, it is part of typing
        generagtor_with_return = Generator(generator)
        for metric in generagtor_with_return:
            yield from self.transform(metric, warnings)
        return generagtor_with_return.value


class TransformationBase(ABC):

    @abstractmethod
    def transform(self, metric, warning=None):
        pass


    def __str__(self):
        '''
        We print a transformrmation just with its class
        '''
        return str(self.__class__.__name__)

    def transform_list(self, generator):
        """
        Takes a generator of metrics and transforms.
        Since the split transformations return, the auxiliary Generator keeps 
        the return value so we can return it.
        to do  "= yield from"
        """
        # TODO: modify the namme Generator, it is part of typing
        generagtor_with_return = Generator(generator)
        for metric in generagtor_with_return:
            yield from self.transform(metric)
        return generagtor_with_return.value

class NotAConverstion(TransformationException):
    pass


class ConverterMixin:
    def convert(self, metric, warnings=None):
        transformation = list(self.transform(metric, warnings))
        if len(transformation) != 1:
            raise Exception("Transformation returns more than one lement")
        return transformation[0]


class BaseConverter(TransformationBase, ConverterMixin):
    '''
    A converter is a transformation that is only one to one
    '''
    pass

class SimpleConversion(BaseConverter):
    '''
    Base for a transformation that basically "casts" one metric to another.
    This could be similar to using  object.__class__ = NewClass, 
    but it looks less hacky.
    Abstract handeling from https://stackoverflow.com/questions/45248243/most-pythonic-way-to-declare-an-abstract-class-property
    '''
    ORIGINAL_CLASS = NotImplemented
    RESULTING_CLASS = NotImplemented

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if cls.ORIGINAL_CLASS is NotImplemented:
            raise NotImplementedError('Please define the original class')
        if cls.RESULTING_CLASS is NotImplemented:
            raise NotImplementedError('Please define the resulting class')

    def transform(cls, metric, warnings=None):
        data = metric.data.copy()
        # Have empty content, if not available.
        if cls.RESULTING_CLASS.content_key not in data:
            data[cls.RESULTING_CLASS.content_key] = []
        return [cls.RESULTING_CLASS(data)]

# Pipe classes
# Helper classes to allow for a more fluid transformation of metrics

@dataclass(frozen=True)
class MetricState:
    '''
    Allows for a fluid conversion of elements.
    Transformations should use TransformationState
    '''
    _metric: Optional[Any]
    warnings: List[Exception] = field(default_factory=lambda: [])
    error: Optional[TransformationException] = None

    @property
    def is_error(self):
        return self.error is not None

    @property
    def metric(self) -> Optional[Any]:
        if self.is_error:
            return None
        return self._metric

    @property
    def error_metric(self):
        if not self.is_error:
            return None
        return self._metric

    def convert(self, converter:BaseConverter) -> "MetricState":
        '''
        Allows to pipe converters keeping the warnings:
        cs.convert(tr).convert(tr).convert(tr)
        '''
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

    def pipeline(self, converters: List[BaseConverter], fail_if_exception=False):
        '''
        Passes the metric state through a series of converters
        '''
        current_metric = self
        for convertr in converters:
            current_metric = current_metric.convert(convertr)
        if fail_if_exception and current_metric.is_error:
            raise current_metric.error
        return current_metric



@dataclass(frozen=True)
class TransformationState:
    metrics_state: List[MetricState]
    warnings: List[Exception] = field(default_factory=lambda: [])

    @classmethod
    def from_metric(cls, metric):
        '''
        Wrap a single metric
        '''
        return cls([MetricState(metric)])


    def transform_gen(self, transformer:TransformationBase, warnings) -> "TransformationState":
        '''
        Allows for pipes of transformations
        '''
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

class TransformationMutable(MetricTransformationBase):

    def __init__(self, warnings):
        if warnings is None:
            warnings = []
        self.warnings = warnings

    def warning(self, warning):
        self.warnings.append(warning)

class StatefulTransformation(MetricTransformationBase):
    '''
    Helper class for the transformations with too many functions, that require access 
    to the warning state (or potential more state in the future).
    '''
    def __init__(self, tranformation_class):
        self.constructor = tranformation_class

    def transform(self, metric, warnings=None):
        if warnigns == None:
            warnings = []
        transformation = self.constructor(*self.args, **self.kargs, warnings=warnings)
        yield from self.transformation.transform(metric)

