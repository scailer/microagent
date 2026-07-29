import logging
import sys
import os

from microagent import configure


cur_dir = os.path.dirname(os.path.realpath(__file__))
configure('file://' + os.path.join(cur_dir, 'signals.json'))
logging.basicConfig(format=(
    '%(levelname)-8s [pid#%(process)d] %(asctime)s %(name)s '
    '%(filename)s:%(lineno)d %(message)s'
), stream=sys.stdout, level=logging.DEBUG)


BUS = {
    'redis': {
        'backend': 'microagent.tools.redis.RedisSignalBus',
        'dsn': 'redis://localhost/7',
        'prefix': 'PREF',
    },
}

BROKER = {
    'redis': {
        'backend': 'microagent.tools.redis.RedisBroker',
        'dsn': 'redis://localhost/7',
    },
}


AGENT = {
    'user_agent': {
        'backend': 'examples.user_agent.UserAgent',
        'bus': 'redis',
        'broker': 'redis',
    },
    'comment_agent': {
        'backend': 'examples.comment_agent.CommentAgent',
        'bus': 'redis',
        'broker': 'redis',
    },
    'email_agent': {
        'backend': 'examples.email_agent.EmailAgent',
        'broker': 'redis',
    },
}
