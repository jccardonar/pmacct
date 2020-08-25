import pytest
from statsd_metrics_exporter import LibratoFormatter, StatsDMetricExporter


class TestLibratoFormatter:
    @pytest.mark.parametrize(
        "name,labels,expected",
        [("test", {2: 3, 4: 5}, "test#2=3,4=5"), ("other_test", {}, "other_test#")],
    )
    def test_format(self, name, labels, expected):
        assert LibratoFormatter().format(name, labels) == expected


class TestStatsDMetricExporter:
    @pytest.mark.parametrize(
        "name,labels,prefix,value,expected",
        [
            ("test", {2: 3, 4: 5}, None, 1, "test#2=3,4=5:1|c"),
            ("other_test", {}, None, -1, "other_test#:-1|c"),
            ("other_test", {}, "etst", -1, "etst.other_test#:-1|c"),
        ],
    )
    def test_format(self, name, labels, prefix, value, expected):
        assert (
            StatsDMetricExporter(prefix=prefix).build_counter(name, value, labels)
            == expected
        )
