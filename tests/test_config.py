from cocaine.burlak import Config, ConsoleLogger
from cocaine.burlak.defaults import Defaults
from cocaine.burlak.mokak.mokak import SharedStatus

import pytest


good_secret_conf = [
    ('tests/assets/conf1.yaml', dict(
        mod='test1',
        client_id=100500,
        client_secret='top secret',
        token_expiration_s=Defaults.TOK_UPDATE_SEC
    )),
    ('tests/assets/conf2.yaml', dict(
        mod='tvm2',
        client_id=42,
        client_secret='not as secret at all',
        token_expiration_s=600
    )),
]

empty_conf = 'tests/assets/empty.conf.yaml'

default_secure = dict(token_expiration_s=Defaults.TOK_UPDATE_SEC)

shared_status = SharedStatus()


@pytest.mark.parametrize(
    'config_file,secure', good_secret_conf)
def test_secure_config(config_file, secure):
    cfg = Config(shared_status)
    cnt = cfg.update([config_file])

    assert cfg.secure == secure
    assert cnt == 1


def test_config_group():
    cfg = Config(shared_status)
    cnt = cfg.update([conf for conf, _ in good_secret_conf])

    assert cfg.secure == good_secret_conf[-1][1]
    assert cnt == len(good_secret_conf)


def test_broken_conf():
    cfg = Config(shared_status)
    cnt = cfg.update([empty_conf])

    assert cnt == 1


def test_config_group_with_broken():
    conf_files = [conf for conf, _ in good_secret_conf]
    conf_files.append(empty_conf)

    cfg = Config(shared_status)
    cnt = cfg.update(conf_files)

    assert cfg.secure == good_secret_conf[-1][1]
    assert cnt == len(good_secret_conf) + 1


def test_config_group_with_broken_and_noexist():
    conf_files = [empty_conf, 'boo/foo.yml']

    cfg = Config(shared_status)
    cnt = cfg.update(conf_files)

    assert cnt == 1
    assert cfg.secure == default_secure


def test_empty_config():
    cfg = Config(shared_status)
    assert cfg.secure == default_secure


@pytest.mark.parametrize(
    'config,expect_port,expect_web_path,expect_uuid_path',
    [
        ('tests/assets/conf1.yaml', 100500, '', Defaults.UUID_PATH),
        ('tests/assets/conf2.yaml', 8877, '/to-heaven', '/some/deep/location'),
    ]
)
def test_endpoints_options(
        config, expect_port, expect_web_path, expect_uuid_path):

    cfg = Config(shared_status)
    cfg.update([config])

    assert (expect_port, expect_web_path) == cfg.web_endpoint
    assert expect_uuid_path == cfg.uuid_path


@pytest.mark.parametrize(
    'config,expect_unicorn_name,expect_node_name',
    [
        ('tests/assets/conf1.yaml', 'unicorn', 'some_other_node'),
        ('tests/assets/conf2.yaml', 'big_unicorn', 'node'),
    ]
)
def test_service_names(config, expect_unicorn_name, expect_node_name):
    cfg = Config(shared_status)
    cfg.update([config])

    assert expect_unicorn_name == cfg.unicorn_name
    assert expect_node_name == cfg.node_name


@pytest.mark.parametrize(
    'config,expect_profile,expect_stop_apps,'
    'expect_expire_stopped,expect_log_level',
    [
        (
            'tests/assets/conf1.yaml', 'default', False,
            Defaults.EXPIRE_STOPPED_SEC, 100
        ),
        (
            'tests/assets/conf2.yaml',
            'isolate_profile',
            True,
            42,
            int(ConsoleLogger.ERROR) + 1
        ),
    ]
)
def test_misc_options(
        config,
        expect_profile, expect_stop_apps,
        expect_expire_stopped, expect_log_level):

    cfg = Config(shared_status)
    cfg.update([config])

    assert cfg.default_profile == expect_profile
    assert cfg.stop_apps == expect_stop_apps
    assert cfg.expire_stopped == expect_expire_stopped
    assert cfg.console_log_level == expect_log_level


@pytest.mark.parametrize(
    'config,status_web_path,status_port',
    [
        ('tests/assets/conf1.yaml', r'/status', 10042),
        ('tests/assets/conf2.yaml', r'/boo', 9878),
    ]
)
def test_extra1(config, status_web_path, status_port):
    cfg = Config(shared_status)
    cfg.update([config])

    assert cfg.status_web_path == status_web_path
    assert cfg.status_port == status_port


@pytest.mark.parametrize(
    'config,apps_poll_interval_sec,input_queue_size',
    [
        ('tests/assets/conf1.yaml', 100500, 1024),
        ('tests/assets/conf2.yaml', Defaults.APPS_POLL_INTERVAL_SEC, 42),
    ]
)
def test_extra2(config, apps_poll_interval_sec, input_queue_size):
    cfg = Config(shared_status)
    cfg.update([config])

    assert cfg.apps_poll_interval_sec == apps_poll_interval_sec
    assert cfg.input_queue_size == input_queue_size


@pytest.mark.parametrize(
    'config,expect_dsn',
    [
        ('tests/assets/conf1.yaml', ''),
        ('tests/assets/conf2.yaml', 'https://100400@some.sentry.org'),
    ]
)
def test_sentry_dsn(config, expect_dsn):
    cfg = Config(shared_status)
    cfg.update([config])

    assert cfg.sentry_dsn == expect_dsn


@pytest.mark.parametrize(
    'config,expect_loc_endp',
    [
        ('tests/assets/conf1.yaml',
            [[Defaults.LOCATOR_HOST, Defaults.LOCATOR_PORT], ]),
        ('tests/assets/conf2.yaml', [['host1', 100500], ['host2', 42]]),
        ('tests/assets/conf3.yaml', [[Defaults.LOCATOR_HOST, 10042]])
    ]
)
def test_locator_endpoints(config, expect_loc_endp):
    cfg = Config(shared_status)
    cfg.update([config])

    assert cfg.locator_endpoints == expect_loc_endp


def test_console_log_level_setter():
    console_level = 100500

    cfg = Config(shared_status)
    cfg.console_log_level = console_level

    assert cfg.console_log_level == console_level


@pytest.mark.parametrize(
    'config,expected_to',
    [
        ('tests/assets/conf1.yaml', Defaults.API_TIMEOUT),
        ('tests/assets/conf2.yaml', Defaults.API_TIMEOUT),
        ('tests/assets/conf3.yaml', 42),
    ]
)
def test_api_timeout(config, expected_to):
    cfg = Config(shared_status)
    cfg.update([config])

    assert cfg.api_timeout == expected_to


@pytest.mark.parametrize(
    'config,path,enabled',
    [
        ('tests/assets/conf1.yaml', '/foo/bar', True),
        ('tests/assets/conf2.yaml', '/foo/bar', False),
        ('tests/assets/conf3.yaml', Defaults.FEEDBACK_PATH, False),
    ]
)
def test_feeadback_conf(config, path, enabled):
    cfg = Config(shared_status)
    cfg.update([config])

    assert cfg.feedback.unicorn_path == path
    assert cfg.feedback.unicorn_feedback == enabled


@pytest.mark.parametrize(
    'config,timeout',
    [
        ('tests/assets/conf1.yaml', 42),
        ('tests/assets/conf2.yaml', Defaults.ON_ASYNC_ERROR_TIMEOUT_SEC),
    ]
)
def test_async_error(config, timeout):
    cfg = Config(shared_status)
    cfg.update([config])

    assert cfg.async_error_timeout_sec == timeout
