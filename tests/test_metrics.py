"""Test metrics submodule.

TODO: checks for network link speed not available for (1) some NICs,
     (2) all NICs
"""
import sys

from cocaine.burlak.config import Config
from cocaine.burlak.context import Context, LoggerSetup
from cocaine.burlak.defaults import Defaults
from cocaine.burlak.metrics.ewma import EWMA
from cocaine.burlak.metrics.exceptions import MetricsException
from cocaine.burlak.metrics.hub import Hub
from cocaine.burlak.metrics.procfs import Cpu, Loadavg, Memory, Network
from cocaine.burlak.metrics.procfs import IfSpeed
from cocaine.burlak.metrics.procfs import ProcfsMetric
from cocaine.burlak.metrics.source import MetricsSource
from cocaine.burlak.metrics.system import SystemMetrics


import pytest

import six

from .common import make_logger_mock


TEST_ALPHA = 0.7

if (sys.version_info < (3, 0)):
    OPEN_TO_PATCH = '__builtin__.open'
else:
    OPEN_TO_PATCH = 'builtins.open'


metrics_poll_result = {
    'cpu': 0.5,
    'mem': 100500,
    'mem.load': 1.2,
    'mem.total': 42,
}

CPU_STAT_EMPTY_CONTENT = ['cpu  0 0 0 0 0 0 0 0 0 0']

CPU_STAT_CONTENT_1 = ['cpu  1 2 3 0 2 0 1 0 0 0']
CPU_STAT_CONTENT_2 = ['cpu  5 4 3 2 2 0 1 0 0 0']

CPU_LOAD = .21260
CPU_USABLE = 0.0708671

MEMINFO_CONTENT_1 = [
    'MemTotal:         100500 kB',
    'MemFree:             128 kB',
    'MemAvailable:         42 kB',
    'Buffers:               1 kB',
    'Cached:             1024 kB',
]

MEMINFO_ZEROES = [
    'MemTotal:              0 kB',
    'MemFree:               0 kB',
    'MemAvailable:          0 kB',
    'Buffers:               0 kB',
    'Cached:                0 kB',
]

MEMINFO_INVALID_CACHED = [
    'MemTotal:            200 kB',
    'MemFree:             100 kB',
    'MemAvailable:          3 kB',
    'Buffers:               1 kB',
    'Cached:              110 kB',
]


MEM_TOTAL_BYTES = 100500 << 10
MEM_FREE_BYTES = 128 << 10
MEM_CACHED_BYTES = 1024 << 10
MEM_USED_BYTES = (MEM_TOTAL_BYTES - MEM_FREE_BYTES)


MEMINFO_CONTENT_2 = [
    'MemTotal:         100501 kB',
    'MemFree:             256 kB',
    'MemAvailable:         32 kB',
]


LOADAVG_CONTENT = [
    '1.02 0.99 1.00 2/1188 9164'
]


NETWORK_CONTENT_1 = [
    'Inter-|   Receive                                        |  Transmit  ',
    'face |bytes    packets errs drop fifo frame compressed multicast'
    '     |bytes    packets errs drop fifo colls carrier compressed',

    '  lan0: 100 10    0    1    0     0          0         0 '
    '         200 10    0    0    0     0       0          0',
    '  lan1: 200 20    0    1    0     0          0         0 '
    '         300 10    0    0    0     0       0          0',
    '  dummy0: 1896120010 6161912    0    1    0     0          0         0 '
    '          206398029  795988    0    0    0     0       0          0'
]


NETWORK_CONTENT_2 = [
    'Inter-|   Receive                                        |  Transmit  ',
    'face |bytes    packets errs drop fifo frame compressed multicast'
    '     |bytes    packets errs drop fifo colls carrier compressed',
    '  lan0: 110 12    0    1    0     0          0         0 '
    '         220 13    0    0    0     0       0          0',
    '  lan1: 230 24    0    1    0     0          0         0 '
    '         340 15    0    0    0     0       0          0',
    '  dummy0: 1896221010 616200    0    1    0     0          0         0 '
    '          20649029  795988    0    0    0     0       0          0'
]

LAN0_SPEED = 100
LAN1_SPEED = 200


class _MockFile(object):
    def __init__(self, data1, data2=None, should_throw=False):
        self._data = data1
        self._data_after = data2

        self._limit = len(data1)
        self._index = 0

        self._should_throw = should_throw

    def seek(self, offset=0):
        # Note: for testing broken simantics - offset is a index,
        # not bytes offset
        self._index = offset

    def readline(self):
        ln = self._data[self._index]
        self._index += 1

        if self._should_throw:
            raise Exception('fuh!')

        if self._index >= self._limit:
            if self._data_after:
                self._data, self._data_after = self._data_after, self._data
            self._index = 0

        return ln

    def readlines(self):
        to_return = self._data

        if self._should_throw:
            raise Exception('fuh!')

        if self._data_after:
            self._data, self._data_after = self._data_after, self._data
        self._index = 0

        return to_return

    def close(self):
        pass

    def __iter__(self):
        return self

    def next(self):
        if self._index >= self._limit:
            if self._data_after:
                self._data, self._data_after = self._data_after, self._data

            raise StopIteration

        ln = self._data[self._index]
        self._index += 1

        return ln


@pytest.fixture
def context(mocker):
    logger = make_logger_mock(mocker)
    sentry_wrapper = mocker.Mock()

    config = Config(mocker.Mock())
    mocker.patch.object(
        Defaults, 'NETLINK_NAME', return_value='lan0')

    return Context(
        LoggerSetup(logger, False),
        config,
        '0',
        sentry_wrapper,
        mocker.Mock(),
    )


def make_open_stub(*args, **kwargs):
    """Generate ProcfsMetric.open method stub."""
    def dummy_open(self, _path):
        self._file = _MockFile(*args, **kwargs)
    return dummy_open


@pytest.fixture
def system_metrics(mocker, context):
    return SystemMetrics(context)


@pytest.fixture
def source(mocker, context):
    return MetricsSource(context, Hub())


def test_source(mocker, source):
    source._system_metrics.poll = \
        mocker.Mock(return_value=metrics_poll_result)

    metrics = source.fetch({})

    assert 'system' in metrics
    assert metrics['system'] == metrics_poll_result


def test_system_metrics(mocker, context):
    """Test system metrics in composition."""
    #
    # TODO(test_system_metrics): refactor this part, it seems ugly.
    #
    state = {'index': 0}
    side_effect = [
        _MockFile(CPU_STAT_CONTENT_1, CPU_STAT_CONTENT_2),
        _MockFile(MEMINFO_CONTENT_1),
        _MockFile(LOADAVG_CONTENT),
        _MockFile(NETWORK_CONTENT_1, NETWORK_CONTENT_2),
    ]

    def dummy_open(self, _path):
        self._file = side_effect[state['index']]
        state['index'] += 1

    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True, side_effect=dummy_open)

    def dummy_get_speed(self, iface):
        if iface == 'lan0':
            return LAN0_SPEED
        elif iface == 'lan1':
            return LAN1_SPEED

        return -100  # unreachable

    mocker.patch.object(
        Network, '_get_link_speed', side_effect=dummy_get_speed, autospec=True)

    system_metrics = SystemMetrics(context)

    system_metrics.poll()
    metrics = system_metrics.poll()

    assert metrics.viewkeys() == {
        'loadavg',
        'cpu.load',
        'cpu.usable',
        'mem.free',
        'mem.cached',
        'mem.load',
        'mem.usable',
        'mem.free_and_cached_ma',
        'mem.used',
        'mem.total',
        'network',
        'poll_inteval_sec',
    }

    eps = .0001

    assert metrics['loadavg'][0] == pytest.approx(1.02, eps)
    assert metrics['loadavg'][1] == pytest.approx(0.99, eps)
    assert metrics['loadavg'][2] == pytest.approx(1.00, eps)

    assert metrics['cpu.load'] == pytest.approx(CPU_LOAD, eps)
    assert metrics['cpu.usable'] == pytest.approx(CPU_USABLE, eps)

    assert metrics['mem.total'] == MEM_TOTAL_BYTES
    assert metrics['mem.free'] == MEM_FREE_BYTES
    assert metrics['mem.cached'] == MEM_CACHED_BYTES

    network = metrics['network']
    assert 'dummy0' not in network

    should_have_iface = ['lan0', 'lan1']
    for iface in should_have_iface:
        assert iface in network

    assert network['lan0']['speed_mbits'] == LAN0_SPEED
    assert network['lan0']['rx'] == 110
    assert network['lan0']['tx'] == 220

    alpha = EWMA.alpha(
        context.config.metrics.poll_interval_sec,
        context.config.metrics.post_interval_sec,
    )
    span = context.config.metrics.poll_interval_sec

    lan0_rx_ma, lan0_tx_ma = EWMA(alpha), EWMA(alpha)
    lan0_rx_ma.update(10 / span)
    lan0_tx_ma.update(20 / span)

    assert network['lan0']['rx_bps'] == lan0_rx_ma.int_of_value
    assert network['lan0']['tx_bps'] == lan0_tx_ma.int_of_value

    lan1_rx_ma, lan1_tx_ma = EWMA(alpha), EWMA(alpha)
    lan1_rx_ma.update(30 / span)
    lan1_tx_ma.update(40 / span)

    assert network['lan1']['speed_mbits'] == LAN1_SPEED

    assert network['lan1']['rx'] == 230
    assert network['lan1']['tx'] == 340

    assert network['lan1']['rx_bps'] == lan1_rx_ma.int_of_value
    assert network['lan1']['tx_bps'] == lan1_tx_ma.int_of_value


def test_system_metrics_exception(mocker, context):
    mocker.patch.object(Cpu, 'read', side_effect=Exception('Boom CPU'))
    mocker.patch.object(Memory, 'read', side_effect=Exception('Boom Mem'))
    mocker.patch.object(Loadavg, 'read', side_effect=Exception('Boom LA'))
    mocker.patch.object(Network, 'read', side_effect=Exception('Boom Net'))

    system_metrics = SystemMetrics(context)

    metrics = system_metrics.poll()
    assert six.viewkeys(metrics) == {'poll_inteval_sec'}


def test_procfs_multiple_times(mocker, context):
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True,
        side_effect=make_open_stub(CPU_STAT_CONTENT_1, CPU_STAT_CONTENT_2))

    p = ProcfsMetric('/profs/stat')

    assert p.read_nfirst_lines(1) == CPU_STAT_CONTENT_1
    assert p.read_nfirst_lines(1) == CPU_STAT_CONTENT_2
    assert p.read_nfirst_lines(1) == CPU_STAT_CONTENT_1


def test_procfs_multiple_lines(mocker, context):
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True,
        side_effect=make_open_stub(MEMINFO_CONTENT_1, MEMINFO_CONTENT_2))

    p = ProcfsMetric('/profs/stat')

    assert p.read_nfirst_lines(1) == MEMINFO_CONTENT_1[:1]
    assert p.read_nfirst_lines(2) == MEMINFO_CONTENT_1[:2]
    assert p.read_nfirst_lines(3) == MEMINFO_CONTENT_1[:3]
    assert p.read_nfirst_lines(5) == MEMINFO_CONTENT_1[:5]
    assert p.read_nfirst_lines(2) == MEMINFO_CONTENT_2[:2]


@pytest.mark.xfail(raises=MetricsException)
def test_cpu_read_exceptions(mocker):
    c = Cpu('/dev/null', TEST_ALPHA)
    c.read()


def test_cpu_empty_record(mocker):
    stub = make_open_stub(CPU_STAT_EMPTY_CONTENT, CPU_STAT_EMPTY_CONTENT)
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True, side_effect=stub)

    c = Cpu('/proc/stat', TEST_ALPHA)
    c = c.read()

    assert isinstance(c.total, int)
    assert isinstance(c.usage, int)
    assert isinstance(c.load, float)

    assert c.total == 0
    assert c.usage == 0
    assert c.load == .0


def test_cpu_overflow(mocker):
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True,
        side_effect=make_open_stub(CPU_STAT_CONTENT_2, CPU_STAT_CONTENT_1))

    source = Cpu('/proc/stat', TEST_ALPHA)
    source.read()
    c = source.read()

    assert isinstance(c.total, int)
    assert isinstance(c.usage, int)
    assert isinstance(c.load, float)
    assert isinstance(c.usable, float)

    assert c.total == 0
    assert c.usage == 0
    assert c.load == .0
    assert c.usable == 1.


def test_mem_zeroes_record(mocker):
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True,
        side_effect=make_open_stub(MEMINFO_ZEROES))

    mocker.patch(OPEN_TO_PATCH, return_value=_MockFile(MEMINFO_ZEROES))

    m = Memory('/proc/stat', TEST_ALPHA)
    m = m.read()

    assert isinstance(m.total, int)
    assert isinstance(m.free, int)
    assert isinstance(m.cached, int)
    assert isinstance(m.used, int)
    assert isinstance(m.load, float)

    assert m.total == 0
    assert m.free == 0
    assert m.cached == 0
    assert m.used == 0
    assert m.load == 0


def test_procfs_metrics(mocker):
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True,
        side_effect=make_open_stub(MEMINFO_CONTENT_1))

    test_path = '/procfs/some/file'

    p = ProcfsMetric(test_path)
    ln = p.read_nfirst_lines(2)
    assert ln == MEMINFO_CONTENT_1[:2]

    ln = p.read_nfirst_lines(2)
    assert ln == MEMINFO_CONTENT_1[:2]

    p.reopen()
    ln = p.read_nfirst_lines(5)
    assert ln == MEMINFO_CONTENT_1

    assert p.path == test_path


@pytest.mark.xfail(raises=Exception)
def test_procfs_metrics_exception(mocker):
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True,
        side_effect=make_open_stub(MEMINFO_CONTENT_1, should_throw=True))

    test_path = '/procfs/some/file'

    p = ProcfsMetric(test_path)
    p.read_nfirst_lines(2)


def test_procfs_metrics_reopen(mocker):
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True,
        side_effect=make_open_stub(MEMINFO_CONTENT_1, should_throw=True))

    p = ProcfsMetric('/procfs/some/file')
    p.reopen = mocker.Mock()

    try:
        p.read_nfirst_lines()
    except Exception:
        pass

    try:
        p.read_all_lines()
    except Exception:
        pass

    assert p.reopen.call_count == 2


@pytest.mark.parametrize('sequence,result', [
    ([0, 1, 2, 3, 4, 5], 4.57),
    ([5, 4, -1, 200, 42], 71.44),
    ([], 0.0),
    ([-1, -2, -3, -2], -2.175),
])
def test_ewma(sequence, result):
    ewma = EWMA(TEST_ALPHA)
    for x in sequence:
        ewma.update(x)
    assert ewma.value == pytest.approx(result, 0.01)


@pytest.mark.xfail(raises=ValueError)
@pytest.mark.parametrize('alpha', [
    (-1),
    (100500),
    (3.14),
    (-100),
])
def test_ewma_broken_alpha(alpha):
    EWMA(alpha)


@pytest.mark.xfail(raises=ValueError)
@pytest.mark.parametrize('dt, tau', [
    (-1, 0.1),
    (0., -2),
    (-10, -20.),
    (0., 0.),
])
def test_ewma_time_constant(dt, tau):
    EWMA.alpha(dt, tau)


@pytest.mark.parametrize('system,apps,metrics', [
    ({'a': 1, 'b': 2}, {}, {'system': {'a': 1, 'b': 2}, 'applications': {}})
])
def test_hub(system, apps, metrics):
    hub = Hub()
    hub.system = system
    hub.applications = apps

    assert hub.system == system
    assert hub.applications == apps

    assert hub.metrics == metrics


@pytest.mark.parametrize('expected', [
    ((['100'], ['200'])),
    ((['100'], ['100'])),
])
def test_ifspeed(mocker, expected):
    mocker.patch.object(
        ProcfsMetric, 'open', autospec=True,
        side_effect=make_open_stub(expected[0], expected[1])
    )

    isp = IfSpeed('/dev/null')

    for sp in expected:
        speed = isp.read()

        assert int(sp[0]) == speed


@pytest.mark.parametrize('a,b,expect', [
    ('a', 'b', 'a/b/' + IfSpeed.SYSFS_FNAME),
    ('z', 'x', 'z/x/' + IfSpeed.SYSFS_FNAME),
])
def test_ifspeed_make_path(a, b, expect):
    assert IfSpeed.make_path(a, b) == expect
