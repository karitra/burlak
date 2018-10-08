"""Main project config."""
import os

import cerberus

from collections import namedtuple

import yaml

from .defaults import Defaults


CONFIG_PATHS = [
    '/etc/cocaine/.cocaine/tools.yml',
    '/etc/cocaine/.cocaine/tools.yaml',
    '/etc/cocaine/orca.yaml',
    # `orca.dynamic.yaml` is used for template dynamic config per:
    # DC, cluster, ect.
    '/etc/cocaine/orca.dynamic.yaml',  # for dynamic sentry config only!
    '~/cocaine/orca.yaml',
    '~/.cocaine/orca.yaml',
]


def make_feedback_config(d):
    FeedbackConfig = namedtuple('FeedbackConfig', [
        'unicorn_path',
        'unicorn_feedback'
    ])

    path = d.get('unicorn_path', Defaults.FEEDBACK_PATH)
    enabled = d.get('unicorn_feedback', False)

    return FeedbackConfig(path, enabled)


def make_metrics_config(d):
    MetricsConfig = namedtuple('MetricsConfig', [
        'path',
        'poll_interval_sec',
        'post_interval_sec',
        'query',
        'enabled',
    ])

    enabled = d.get('enabled', Defaults.METRICS_ENABLED)

    path = d.get('path', Defaults.METRICS_PATH)

    poll_interval_sec = d.get(
        'poll_interval_sec', Defaults.METRICS_POLL_INTERVAL_SEC)
    post_interval_sec = d.get(
        'post_interval_sec', Defaults.METRICS_POST_INTERVAL_SEC)

    query = d.get('query', {})

    return MetricsConfig(
        path, poll_interval_sec, post_interval_sec, query, enabled)


def make_sharding_config(d):
    ShardingConfig = namedtuple('ShardingConfig', [
        'enabled',
        'default_tag',
        'common_prefix',
        'tag_key',
        'state_subnode',
        'feedback_subnode',
        'metrics_subnode',
        'semaphore_subnode',
    ])

    enabled = d.get('enabled', Defaults.SHARDING_ENABLED)

    default_tag = d.get('default_tag', Defaults.FALLBACK_SHARDING_TAG)
    common_prefix = d.get('common_prefix', Defaults.SHARDING_COMMON_PREFIX)

    tag_key = d.get('tag_key', Defaults.DC_TAG_KEY)

    state_subnode = d.get('state_subnode', Defaults.SHARDING_STATE_SUBNODE)
    feedback_subnode = d.get(
        'feed_subnode', Defaults.SHARDING_FEEDBACK_SUBNODE)

    # TODO: probably deprecated
    metrics_subnode = d.get(
        'metrics_subnode', Defaults.SHARDING_METRICS_SUBNODE)

    semaphore_subnode = d.get(
        'semaphore_subnode', Defaults.SHARDING_SEMAPHORE_SUBNODE)

    return ShardingConfig(
        enabled, default_tag, common_prefix, tag_key,
        state_subnode, feedback_subnode, metrics_subnode, semaphore_subnode
    )


def make_netlink_config(d):
    """Construct netlink config record."""
    Netlink = namedtuple('Netlink', [
        'default_name',
        'speed_mbits',
    ])

    default_name = d.get('default_name', Defaults.NETLINK_NAME)
    speed_mbits = d.get('speed_mbits', Defaults.NETLINK_SPEED_MBITS)

    return Netlink(
        default_name,
        speed_mbits,
    )


def make_semaphore_config(d):
    """Construct semaphore config."""
    SemaphoreConfig = namedtuple('SemaphoreConfig', [
        'locks_path',
        'lock_name',
        'locks_count',
        'try_timeout_sec',
    ])

    return SemaphoreConfig(
        locks_path=d.get('path', Defaults.SEMAPHORE_PATH),
        lock_name=d.get('lock_name', Defaults.SEMAPHORE_LOCK_NAME),
        locks_count=d.get('locks_count', Defaults.SEMAPHORE_LOCKS_COUNT),
        try_timeout_sec=d.get(
            'try_timeout_sec', Defaults.SEMAPHORE_TRY_LOCK_SEC),
    )


#
# Should be compatible with tools secure section
#
class Config(object):
    """App-wide config."""
    TASK_NAME = 'config'

    # TODO: make schema work with tools config
    SCHEMA = {
        'locator': {
            'type': 'dict',
            'required': False,
            'schema': {
                'host': {'type': 'string'},
                'port': {
                    'type': 'integer',
                    'min': 0,
                    'max': 2**16
                }
            }
        },
        'secure': {
            'type': 'dict',
            'required': False,
            'schema': {
                'mod': {
                    'type': 'string',
                    'allowed': [
                        'tvm',
                        'tvm2',
                        'TVM',
                        'TVM2',
                        'promiscuous',
                        'test1', 'test2'
                    ]
                },
                'client_id': {'type': 'integer'},
                'client_secret': {'type': 'string'},
                'token_expiration_s': {'type': 'integer', 'required': False},
            },
        },
        'unicorn_service_name': {
            'type': 'string',
            'required': False,
        },
        'node_service_name': {
            'type': 'string',
            'required': False,
        },
        'metrics_service_name': {
            'type': 'string',
            'required': False,
        },
        'port': {
            'type': 'integer',
            'min': 0,
            'required': False,
        },
        'web_path': {
            'type': 'string',
            'required': False,
        },
        'uuid_path': {
            'type': 'string',
            'required': False,
        },
        'feedback': {
            'type': 'dict',
            'required': False,
            'schema': {
                'unicorn_path': {'type': 'string'},
                'unicorn_feedback': {'type': 'boolean'}
            }
        },
        'sharding': {
            'type': 'dict',
            'required': False,
            'schema': {
                'enabled': {'type': 'boolean'},
                'default_tag': {
                    'type': 'string',
                    'required': False,
                },
                'tag_key': {
                    'type': 'string',
                    'required': False,
                },
                'state_subnode': {
                    'type': 'string',
                    'required': False,
                },
                'feedback_subnode': {
                    'type': 'string',
                    'required': False,
                },
                'metrics_subnode': {
                    'type': 'string',
                    'required': False,
                },
                'semaphore_subnode': {
                    'type': 'string',
                    'required': False,
                },

            }
        },
        'metrics': {
            'type': 'dict',
            'required': False,
            'schema': {
                'enabled': {'type': 'boolean'},
                'path': {
                    'type': 'string',
                    'required': False,
                },
                'poll_interval_sec': {
                    'type': 'integer',
                    'min': 0,
                    'max': 2**16,
                    'required': False,
                },
                'post_interval_sec': {
                    'type': 'integer',
                    'min': 0,
                    'max': 2**16,
                    'required': False,
                },
                'query': {
                    'type': 'dict',
                    'required': False,
                },
            },
        },
        'async_error_timeout_sec': {
            'type': 'integer',
            'min': 0,
            'max': 2**16,
            'required': False,
        },
        'default_profile': {
            'type': 'string',
            'required': False,
        },
        'stop_apps': {
            'type': 'boolean',
            'required': False,
        },
        # TODO(Config): deprecated, remove!
        'stop_by_control': {
            'type': 'boolean',
            'required': False,
        },
        # TODO(Config): deprecated, remove!
        'control_with_ack': {
            'type': 'boolean',
            'required': False,
        },
        'pending_stop_in_state': {
            'type': 'boolean',
            'required': False,
        },
        'sentry_dsn': {
            'type': 'string',
            'required': False,
        },
        # TODO(Validator): add `_sec` suffix and make app-wide update
        'expire_stopped': {
            'type': 'integer',
            'required': False,
        },
        'console_log_level': {  # see ConsoleLogger.LEVELS for possible values
            'type': 'integer',
            'required': False,
        },
        'api_timeout_sec': {
            'type': 'integer',
            'min': 0,
            'max': 2**16,
            'required': False,
        },
        'status_web_path': {
            'type': 'string',
            'required': False,
        },
        'status_port': {
            'type': 'integer',
            'min': 0,
            'max': 2**16,
            'required': False,
        },
        'apps_poll_interval_sec': {
            'type': 'integer',
            'min': 0,
            'required': False,
        },
        'input_queue_size': {
            'type': 'integer',
            'min': 0,
            'required': False,
        },
        'locator_endpoints': {
            'type': 'list',
            'required': False,
            'schema': {
                'type': 'list',
                'items': [
                    {'type': 'string'},   # host
                    {'type': 'integer'},  # port
                ],
            },
        },
        'netlink': {
            'type': 'dict',
            'required': False,
            'schema': {
                'speed_mbits': {  # in megabits!
                    'type': 'integer',
                    'min': 0,
                    'max': 2**16,
                    'required': False,
                },
                'default_name': {
                    'type': 'string',
                    'required': False,
                },
            },
        },
        'run.semaphore': {
            'type': 'dict',
            'required': False,
            'schema': {
                'locks_path': {
                    'type': 'string',
                    'required': False,
                },
                'lock_name': {
                    'type': 'string',
                    'required': False,
                },
                'locks_count': {
                    'type': 'integer',
                    'min': 0,
                    'max': 8 * 1024,
                },
                'try_timeout_sec': {
                    'type': 'integer',
                    'min': 0,
                    'max': 2**16,
                    'required': False,
                },
            }
        }
    }

    def __init__(self, shared_status, logger=None):
        self._config = dict()
        self._validator = cerberus.Validator(self.SCHEMA, allow_unknown=True)
        self._logger = logger

        self._status = shared_status.register(Config.TASK_NAME)

    def _validate_raise(self, config):
        if not self._validator.validate(config):  # pragma nocover
            raise Exception('incorrect config format')

    def update(self, paths=CONFIG_PATHS):
        self._status.mark_ok('reading config')
        parsed = []
        for conf in paths:
            try:
                with open(os.path.expanduser(conf)) as fl:
                    print('Reading config from file {}'.format(conf))

                    config = yaml.safe_load(fl.read())
                    if config:
                        # TODO: temporary disabled
                        self._validate_raise(config)
                        self._config.update(config)

                    parsed.append(conf)
            except Exception as err:
                self.err_to_logger(
                    'failed to read config file {}, {}'.format(conf, err),
                    True)

        if not parsed:  # pragma nocover
            info_message = 'no config was found in file(s), using defaults.'
            self._info_to_logger(info_message, True)
            self._status.mark_ok(info_message)
        else:
            self._info_to_logger(
                'config has been updated from file(s) {}'.format(parsed),
                True)
            self._status.mark_ok(
                'config has been updated from {} file(s)'.format(len(parsed)))

        return len(parsed)

    @property
    def config(self):  # pragma nocover
        return self._config

    @property
    def secure(self):
        secure_conf = self._config.get('secure', {})
        secure_conf.setdefault('token_expiration_s', Defaults.TOK_UPDATE_SEC)

        return secure_conf

    @property
    def web_endpoint(self):
        port = self._config.get('port', Defaults.WEB_PORT)
        path = self._config.get('web_path', Defaults.WEB_PATH)

        return port, path

    @property
    def uuid_path(self):
        return self._config.get('uuid_path', Defaults.UUID_PATH)

    @property
    def node_name(self):
        return self._config.get(
            'node_service_name', Defaults.NODE_SERVICE_NAME)

    @property
    def unicorn_name(self):
        return self._config.get(
            'unicorn_service_name', Defaults.UNICORN_SERVICE_NAME)

    @property
    def metrics_name(self):
        return self._config.get(
            'metrics_service_name', Defaults.METRICS_SERVICE_NAME)

    @property
    def default_profile(self):
        return self._config.get(
            'default_profile', Defaults.PROFILE_NAME)

    @property
    def locator_endpoints(self):
        default_host, default_port = \
            Defaults.LOCATOR_HOST, Defaults.LOCATOR_PORT

        locator_section = self._config.get('locator')
        if locator_section and isinstance(locator_section, dict):
            default_host = locator_section.get('host', Defaults.LOCATOR_HOST)
            default_port = locator_section.get('port', Defaults.LOCATOR_PORT)

        return self._config.get(
            'locator_endpoints',
            [[default_host, default_port], ])

    @property
    def stop_apps(self):
        return self._config.get(
            'stop_apps',
            Defaults.STOP_APPS_NOT_IN_STATE)

    @property
    def sentry_dsn(self):
        return self._config.get('sentry_dsn', Defaults.SENTRY_DSN)

    @property
    def expire_stopped(self):
        return self._config.get(
            'expire_stopped', Defaults.EXPIRE_STOPPED_SEC)

    @property
    def console_log_level(self):
        return self._config.get(
            'console_log_level', Defaults.CONSOLE_LOGGER_LEVEL)

    @property
    def apps_poll_interval_sec(self):
        return self._config.get(
            'apps_poll_interval_sec', Defaults.APPS_POLL_INTERVAL_SEC)

    @property
    def async_error_timeout_sec(self):
        return self._config.get(
            'async_error_timeout_sec', Defaults.ON_ASYNC_ERROR_TIMEOUT_SEC)

    @property
    def input_queue_size(self):
        return self._config.get(
            'input_queue_size', Defaults.INPUT_QUEUE_SIZE)

    @property
    def pending_stop_in_state(self):
        return self._config.get(
            'pending_stop_in_state', Defaults.PENDING_STOP_IN_STATE)

    @pending_stop_in_state.setter
    def pending_stop_in_state(self, flag):
        self._config['pending_stop_in_state'] = flag

    @console_log_level.setter
    def console_log_level(self, level):
        self._config['console_log_level'] = level

    @property
    def status_web_path(self):
        return self._config.get('status_web_path', Defaults.STATUS_WEB_PATH)

    @property
    def status_port(self):
        return self._config.get('status_port', Defaults.STATUS_PORT)

    @property
    def feedback(self):
        feedback = self._config.get('feedback', {})
        return make_feedback_config(feedback)

    @property
    def metrics(self):
        metrics = self._config.get('metrics', {})
        return make_metrics_config(metrics)

    @property
    def sharding(self):
        sharding = self._config.get('sharding', {})
        return make_sharding_config(sharding)

    @property
    def api_timeout(self):
        return self._config.get('api_timeout_sec', Defaults.API_TIMEOUT)

    @property
    def api_timeout_by2(self):
        return 2 * self.api_timeout

    @property
    def api_timeout_by4(self):
        return 4 * self.api_timeout

    @property
    def procfs_stat_path(self):
        return Defaults.PROCFS_STAT

    @property
    def procfs_mem_path(self):
        return Defaults.PROCFS_MEMINFO

    @property
    def procfs_loadavg_path(self):
        return Defaults.PROCFS_LOADAVG

    @property
    def procfs_netstat_path(self):
        return Defaults.PROCFS_NETSTAT

    @property
    def sysfs_network_prefix(self):
        return Defaults.SYSFS_NET_PREFIX

    @property
    def netlink(self):
        netlink = self._config.get('netlink', {})
        return make_netlink_config(netlink)

    @property
    def semaphore(self):
        semaphore = self._config.get('run.semaphore', {})
        return make_semaphore_config(semaphore)

    # TODO:
    #   refactor to single method?
    #   make *args format
    def err_to_logger(self, msg, to_console=False):  # pragma nocover
        if self._logger:
            self._logger.error(msg)
        self._dump_to_console(msg, to_console)

    # TODO:
    #   refactor to single method?
    #   make *args format
    def _info_to_logger(self, msg, to_console=False):  # pragma nocover
        if self._logger:
            self._logger.info(msg)
        self._dump_to_console(msg, to_console)

    def _dump_to_console(self, msg, to_console=False):  # pragma nocovers
        if to_console:
            print (msg)
