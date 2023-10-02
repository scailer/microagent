BUS = {
    'default': {
        'backend': 'microagent.tools.redis.RedisSignalBus',
        'dsn': 'redis://localhost',
        'prefix': 'pref',
    }
}


AGENT = {
    'user_agent': {
        'backend': 'user_agent.UserAgent',
        'bus': 'default',
    },
}
