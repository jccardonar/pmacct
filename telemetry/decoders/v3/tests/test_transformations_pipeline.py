import pytest
from base_transformation import TransformationException, TransformationState, MetricState, TransformationBase, BaseConverter
from metric_types.base_types import DictSubTreeData

class WarningNumberMatched(TransformationException):
    pass

class Transformtionifmatch(BaseConverter):

    def __init__(self, key, value):
        if value is None:
            raise Exception("Value cannot be None")
        self.key = key
        self.value = value
        super().__init__()

class TransformationWarningIf(Transformtionifmatch):
    '''
    Throws a warning if content includes 
    '''
    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        if metric.content and isinstance(metric.content, dict) and metric.content.get(self.key, None) == self.value:
            warnings.append(WarningNumberMatched("Found value", {"value": self.value}))
        yield  metric

class TransformationErrorIf(Transformtionifmatch):
    '''
    Throws a warning if content includes 
    '''
    def transform(self, metric, warnings=None):
        if warnings is None:
            warnings = []
        if metric.content and isinstance(metric.content, dict) and metric.content.get(self.key, None) == self.value:
            raise WarningNumberMatched("Found value", {"value": self.value})
        yield metric

class MultiplyMetric(TransformationBase):

    def __init__(self, number, function, default):
        self.number = int(number)
        self.function = function
        self.default = default

    def transform(self, metric, warnings=None):
        for n in range(1, self.number+1):
            data = metric.content.copy()
            data["number"] = self.function(data.get("number", self.default), n)
            yield metric.replace(content=data)

class ConverterMetric(BaseConverter):
    def transform(self, metric, warnings=None):
        data = metric.content.copy()
        data["number"] = data.get("number", 0) + 1
        yield metric.replace(content=data)

class MetricDummy(DictSubTreeData):

    def __init__(self, data=None):
        if data is None:
            data = {"content": {}}
        super().__init__(data)

    def __repr__(self):
        return ''.join([self.__class__.__name__, f"(data={self.data})"])

    @property
    def number(self):
        return self.content.get("number", None)

class TestTransformation:

    def test_basic(self):
        metric = MetricDummy()
        metrics = list(MultiplyMetric(2, lambda x,y: x * y, 1).transform(metric))


    def test_pipe_transform(self):
        # send a simple metric to multiple transformations getting something expected, with and without warnings. With and without errors.
        t_multiplier_2 = MultiplyMetric(2, lambda x,y: x * y, 1)
        t_sums_1 = ConverterMetric()
        warning_if_three = TransformationWarningIf("number", 3)
        error_if_three = TransformationErrorIf("number", 3)
        metric = MetricDummy()

        assert metric.number == None
        state = TransformationState.from_metric(metric).transform(t_multiplier_2).transform(t_sums_1).transform(t_multiplier_2)
        assert [x.metric.number for x in state.metrics_state] == [2,4,3,6]
        assert not state.warnings
        pipeline = [t_multiplier_2, t_sums_1, t_multiplier_2]
        state = TransformationState.from_metric(metric).pipeline(pipeline)
        assert [x.metric.number for x in state.metrics_state] == [2,4,3,6]
        assert not state.warnings


        assert metric.number == None
        state = TransformationState.from_metric(metric).transform(t_multiplier_2).transform(t_sums_1).transform(warning_if_three).transform(t_multiplier_2)
        assert [x.metric.number for x in state.metrics_state] == [2,4,3,6]
        assert state.warnings
        pipeline = [t_multiplier_2, t_sums_1, warning_if_three, t_multiplier_2]
        state = TransformationState.from_metric(metric).pipeline(pipeline)
        assert [x.metric.number for x in state.metrics_state] == [2,4,3,6]
        assert state.warnings

        assert metric.number == None
        state = TransformationState.from_metric(metric).transform(t_multiplier_2).transform(t_sums_1).transform(error_if_three).transform(t_multiplier_2)
        #assert [x.metric.number for x in state.metrics_state] == [2,4,3,6]
        #assert state.warnings
        assert [x.metric.number for x in state.metrics_state if x.metric] == [2,4]
        assert None in [x.metric for x in state.metrics_state] 
        pipeline = [t_multiplier_2, t_sums_1, error_if_three, t_multiplier_2]
        state = TransformationState.from_metric(metric).pipeline(pipeline)
        assert [x.metric.number for x in state.metrics_state if x.metric] == [2,4]
        assert None in [x.metric for x in state.metrics_state] 



    def test_pipe_convert(self):
        # send a simple metric to multiple conversions getting something expected. Without and with warnings. ALso raising an exception.
        converter = ConverterMetric()
        warning_if_two = TransformationWarningIf("number", 2)
        error_if_two = TransformationErrorIf("number", 2)
        metric = MetricDummy()
        assert metric.number == None
        new_metric = MetricState(metric).convert(converter).convert(converter).metric
        assert new_metric
        assert new_metric.number == 2

        assert metric.number == None
        new_state = MetricState(metric).convert(converter).convert(warning_if_two).convert(converter)
        assert new_state.metric
        new_metric = new_state.metric
        assert new_metric.number == 2
        assert not new_state.warnings

        assert metric.number == None
        new_state = MetricState(metric).convert(converter).convert(converter).convert(warning_if_two).convert(converter)
        assert new_state.metric
        new_metric = new_state.metric
        assert new_metric.number == 3
        assert new_state.warnings


        assert metric.number == None
        new_state = MetricState(metric).convert(converter).convert(error_if_two).convert(converter)
        assert new_state.metric
        new_metric = new_state.metric
        assert new_metric.number == 2
        assert not new_state.warnings

        assert metric.number == None
        new_state = MetricState(metric).convert(converter).convert(converter).convert(error_if_two).convert(converter)
        assert new_state.metric is None
        assert new_state.is_error
        assert new_state.error_metric.number == 2
        assert new_state.error is not None and new_state.error.fields["tranformation"]

        with pytest.raises(WarningNumberMatched):
            MetricState(metric).convert(converter).convert(converter).convert_raiser_error(error_if_two).convert(converter)

        pipeline = [converter, converter, error_if_two, converter]
        
        new_state = MetricState(metric).pipeline(pipeline)
        assert new_state.metric is None
        assert new_state.is_error
        assert new_state.error_metric.number == 2
        assert new_state.error is not None and new_state.error.fields["tranformation"]

        with pytest.raises(WarningNumberMatched):
            MetricState(metric).pipeline(pipeline, True)

