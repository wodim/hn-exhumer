import configparser
from html import unescape
import json
import logging
from time import time

import requests


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class HN:
    NEW_URL = 'https://hacker-news.firebaseio.com/v0/newstories.json'
    ITEM_URL = 'https://hacker-news.firebaseio.com/v0/item/%s.json'

    def __init__(self):
        self.session = requests.Session()
        self.mem = configparser.ConfigParser()
        self.mem.read('memory.ini')
        if 'memory' not in self.mem:
            self.mem['memory'] = {}
        if 'status' not in self.mem:
            self.mem['status'] = dict(last_gc=0)

    def request(self, url):
        logger.info('requesting url: %s', url)
        data = self.session.get(url).text
        return json.loads(data)

    @staticmethod
    def clean_text(text):
        return unescape(text).replace('<p>', '\n\n')

    def _get_story_memory(self, story_id):
        # logger.info('retrieving from memory: %s', story_id)
        try:
            value, _ = self.mem['memory'][story_id].split(',')
        except (ValueError, KeyError):
            value = 'new'
        return value

    def _put_story_memory(self, story_id, value):
        # logger.info('saving to memory: %s=%s', story_id, value)
        self.mem['memory'][str(story_id)] = '%s,%s' % (value, int(time()))

    def _flush_memory(self):
        if int(self.mem['status']['last_gc']) + 60 * 30 < time():
            self._gc_memory()

        with open('memory.ini', 'w', encoding='utf8') as fp:
            self.mem.write(fp)

    def _gc_memory(self):
        logger.info('running gc')
        for key in self.mem['memory']:
            _, time_ = self.mem['memory'][key].split(',')
            # garbage collect this one if it's older than 2 days
            if int(time_) + 60 * 60 * 24 * 3 < time():
                logger.info('%s: garbage collecting', key)
                del self.mem['memory'][key]
                continue
        self.mem['status']['last_gc'] = str(int(time()))

    def get_updates(self):
        story_ids = [str(x) for x in self.request(self.NEW_URL)]

        for key in self.mem['memory']:
            state = self._get_story_memory(key)

            if key in story_ids:
                story_ids.remove(key)
                if state == 'dead':
                    # this one was dead and now it is not
                    logger.info('%s: story resurrected', key)
                    story = self.request(self.ITEM_URL % key)
                    yield (story, 'resurrected')
                elif state == 'old':
                    # benjamin button vibes
                    pass
                self._put_story_memory(key, 'new')
                continue

            if state != 'new':
                # we'll save these for now until they are garbage collected
                continue

            logger.info('%s: story disappeared...', key)
            story = self.request(self.ITEM_URL % key)
            if not story:
                # some stories are on the list of new stories but they
                # never get pushed to the api; the api just returns "null"
                logger.info('%s: nulled?', key)
                self._put_story_memory(key, 'old')
                continue

            if story.get('deleted'):
                # forget deleted stories
                logger.info('%s: deleted', key)
                self._put_story_memory(key, 'old')
                continue

            # find out why this story vanished
            if story.get('dead'):
                # got killed
                logger.info('%s: ...because it was killed', key)
                yield (story, 'killed')
                self._put_story_memory(key, 'dead')
            elif story.get('deleted'):
                # was deleted by the user
                logger.info('%s: ...because it was deleted', key)
                # yield (story, 'deleted')
                self._put_story_memory(key, 'deleted')
            else:
                # this one got too old so it does not show on the list
                # of new stories.
                logger.info('%s: ...because it is too old', key)
                self._put_story_memory(key, 'old')

        for story_id in story_ids:
            # the ones that weren't in memory are new
            logger.info('%s: adding new story', story_id)
            self._put_story_memory(story_id, 'new')

        self._flush_memory()
