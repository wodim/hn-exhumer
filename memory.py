import logging
import pickle
from time import time


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Memory:
    EXPIRY = 60 * 60 * 24

    def __init__(self):
        try:
            with open('memory.pickle', 'rb') as fp:
                self.mem = pickle.load(fp)
        except:
            self.mem = {}

    def get_value(self, story_id, key):
        logger.debug('retrieving from memory: %s->%s', story_id, key)
        try:
            return self.mem[story_id][key]
        except KeyError:
            return None

    def get_last_value(self, story_id, key):
        logger.debug('retrieving last from memory: %s->%s', story_id, key)
        try:
            return self.mem[story_id][key][-1]
        except KeyError:
            return None

    def put_value(self, story_id, key, value):
        logger.debug('saving to memory: %s->%s=%s', story_id, key, value)
        if story_id not in self.mem:
            self.mem[story_id] = {}
        self.mem[story_id][key] = value
        self.mem[story_id]['time'] = time()

    def put_set_value(self, story_id, key, value):
        logger.debug('saving to set memory: %s->%s=%s', story_id, key, value)
        if story_id not in self.mem:
            self.mem[story_id] = {}
        if key not in self.mem[story_id]:
            self.mem[story_id][key] = [value]
        else:
            self.mem[story_id][key].append(value)
        self.mem[story_id]['time'] = time()

    def get_all_ids(self):
        logger.debug('getting all ids')
        return self.mem.keys()

    def rank_history(self, story_id):
        logger.debug('getting rank history for %s', story_id)
        try:
            return self.mem[story_id]['rank']
        except KeyError:
            return []

    def flush_memory(self):
        logger.debug('flushing memory')
        with open('memory.pickle', 'wb') as fp:
            pickle.dump(self.mem, fp)
