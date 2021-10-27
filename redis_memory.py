import logging
from time import time

import redis


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class RedisMemory:
    KEY_VALUE = 'hn_%s_%s'
    KEY_PATTERN = 'hn_*'
    KEY_RANK_PATTERN = 'hn_*_rank'
    EXPIRY = 60 * 60 * 24

    def __init__(self):
        self.redis = redis.Redis()

    def get_value(self, story_id, key):
        logger.debug('retrieving from memory: %s->%s', story_id, key)
        return self.redis.get(self.KEY_VALUE % (story_id, key))

    def get_last_value(self, story_id, key):
        logger.debug('retrieving last from memory: %s->%s', story_id, key)
        try:
            return self.redis.zrangebyscore(self.KEY_VALUE % (story_id, key),
                                           '-inf', '+inf', start=0, num=1)[1]
        except IndexError:
            return None

    def put_value(self, story_id, key, value):
        logger.debug('saving to memory: %s->%s=%s', story_id, key, value)
        return self.redis.setex(self.KEY_VALUE % (story_id, key), self.EXPIRY, value)

    def put_set_value(self, story_id, key, value):
        logger.debug('saving to set memory: %s->%s=%s', story_id, key, value)
        redis_key = self.KEY_VALUE % (story_id, key)
        self.redis.zadd(redis_key, {time(): value})
        self.redis.expire(redis_key, self.EXPIRY)

    def get_all_ids(self):
        logger.debug('getting all ids')
        return [int(x.decode('utf8').split('_')[1]) for x in self.redis.keys(self.KEY_PATTERN)]

    def rank_history(self, story_id):
        logger.debug('getting rank history for %s', story_id)
        return self.redis.zrangebyscore(self.KEY_VALUE % (story_id, 'rank'),
                                        time() - self.EXPIRY, '+inf')

    def flush_memory(self):
        logger.debug('flushing memory')
        for key in self.redis.keys(self.KEY_RANK_PATTERN):
            self.redis.zremrangebyscore(key, '-inf', time() - self.EXPIRY)
