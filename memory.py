import logging
import pickle
from time import time


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Memory:
    EXPIRY = 60 * 60 * 24 * 3
    mem = {}
    persistent = False

    def __init__(self, persistent=True):
        if not persistent:
            return
        try:
            with open('memory.pickle', 'rb') as fp:
                self.mem = pickle.load(fp)
            self.persistent = True
        except Exception:
            logger.exception('error loading pickled database! starting with an empty one.')
            logger.warning("I won't persist memory to disk - all your data will be lost")

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
        except (IndexError, KeyError):
            return None

    def put_value(self, story_id, key, value):
        logger.debug('saving to memory: %s->%s=%s', story_id, key, value)
        if story_id not in self.mem:
            self.mem[story_id] = {}
        self.mem[story_id][key] = value
        self.mem[story_id]['time'] = time()

    def put_list_value(self, story_id, key, value, limit=None):
        logger.debug('saving to list memory: %s->%s=%s', story_id, key, value)
        if story_id not in self.mem:
            self.mem[story_id] = {}
        if key not in self.mem[story_id]:
            self.mem[story_id][key] = [value]
        else:
            self.mem[story_id][key].append(value)
            if limit:
                # if a limit of values was specified, truncate the list
                # pylint: disable=E1130
                self.mem[story_id][key] = self.mem[story_id][key][-limit:]
        self.mem[story_id]['time'] = time()

    def get_all_keys(self):
        logger.debug('getting all keys')
        return self.mem.keys()

    def flush_memory(self):
        logger.debug('flushing memory')
        for story_id in list(story_id for story_id in self.mem
                             if self.mem[story_id]['time'] < time() - self.EXPIRY):
            del self.mem[story_id]
        if not self.persistent:
            logger.info('WARNING: memory is not persistent - all your data will be lost!')
            return
        with open('memory.pickle', 'wb') as fp:
            pickle.dump(self.mem, fp)
