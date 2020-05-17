import sys
import logging

logging.basicConfig(
format='%(levelname)-8s [pid#%(process)d] %(asctime)s %(name)s %(filename)s:%(lineno)d %(message)s',
stream=sys.stdout, level=logging.DEBUG)


BUS = {
    'aioredis': {
        'backend': 'microagent.tools.aioredis.AIORedisSignalBus',
        'dsn': 'redis://localhost/7',
        'prefix': 'PREF',
    },
}

BROKER = {
    'aioredis': {
        'backend': 'microagent.tools.aioredis.AIORedisBroker',
        'dsn': 'redis://localhost/7',
    },
}


AGENT = {
    'user_agent': {
        'backend': 'examples.user_agent.UserAgent',
        'bus': 'aioredis',
        'broker': 'aioredis',
    },
    'comment_agent': {
        'backend': 'examples.comment_agent.CommentAgent',
        'bus': 'aioredis',
        'broker': 'aioredis',
    },
    'email_agent': {
        'backend': 'examples.email_agent.EmailAgent',
        'broker': 'aioredis',
    },
}
