from html import unescape
import json
import logging
from time import time

import requests

from memory import Memory


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class HN:
    NEW_URL = 'https://hacker-news.firebaseio.com/v0/newstories.json'
    TOP_URL = 'https://hacker-news.firebaseio.com/v0/topstories.json'
    ITEM_URL = 'https://hacker-news.firebaseio.com/v0/item/%d.json'
    ITEM_PERMALINK = 'https://news.ycombinator.com/item?id=%d'
    PAGE_SIZE = 30
    NON_CACHEABLE_META = ('dead', 'deleted', 'score', 'descendants', 'kids')

    def __init__(self):
        self.session = requests.Session()
        self.memory = Memory()

    def request(self, url):
        logger.info('requesting url: %s', url)
        data = self.session.get(url, timeout=3).text
        return json.loads(data)

    @staticmethod
    def clean_text(text):
        return unescape(text).replace('<p>', '\n\n')

    @classmethod
    def get_permalink(cls, story_id):
        return cls.ITEM_PERMALINK % story_id

    def _get_rank_change(self, story_id):
        rank_data = self.memory.get_value(story_id, 'rank') or []
        if len(rank_data) < 2:
            # can only check for a change if there are two samples
            return 0
        if rank_data[0][1] < time() - 30:
            # if the "old" sample is older than 30 seconds, it's useless.
            # this can happen if the bot stopped. destroy the whole thing
            self.memory.put_value(story_id, 'rank', [])
            return 0
        rank_history = tuple(x for x, _ in rank_data)
        if (rank_history[-2] <= self.PAGE_SIZE and
                rank_history[-1] > self.PAGE_SIZE * 2):
            # a story that was on the front page and suddently is in the third
            # page (at least) was downranked
            self.memory.put_value(story_id, 'downranked', 1)
            return -1
        if (rank_history[-1] <= self.PAGE_SIZE and
                self.memory.get_value(story_id, 'downranked') == 1):
            # a story that was downranked and then returns to the front page
            # was dedownranked
            self.memory.put_value(story_id, 'downranked', 0)
            return 1
        if (rank_history[-1] <= self.PAGE_SIZE and
                rank_history[-2] > self.PAGE_SIZE * 2 and
                self._get_story_meta(story_id, 'time') < time() - 60 * 60 * 6):
            # a story that was in the third page (at least) for over six hours
            # and then suddently shows on the front page was put there manually
            return 2
        return 0

    def _get_story_meta(self, story_id, key):
        if key in self.NON_CACHEABLE_META:
            logger.debug('getting story meta (non-cacheable) %s->%s', story_id, key)
        else:
            logger.debug('getting story meta %s->%s', story_id, key)
        return self._get_story_data(story_id, key in self.NON_CACHEABLE_META).get(key)

    def _get_story_data(self, story_id, force=False):
        if self.memory.get_value(story_id, 'nulled'):
            return {}
        data = self.memory.get_value(story_id, 'data') or {}
        if force or not data:
            # we update instead of replacing so we can keep meta from
            # deleted stories
            if live_data := self.request(self.ITEM_URL % story_id):
                data.update(live_data)
                self.memory.put_value(story_id, 'data', data)
            else:
                self.memory.put_value(story_id, 'nulled', True)
        return data

    def get_updates(self):
        new_story_ids = self.request(self.NEW_URL)

        for story_id in self.memory.get_all_keys():
            state = self.memory.get_value(story_id, 'state')

            if not state:
                # some stories are on the list of new stories but they
                # never get pushed to the api; the api just returns "null"
                logger.info('%s: nulled?', story_id)
                self.memory.put_value(story_id, 'state', 'old')
                continue

            if story_id in new_story_ids:
                # remove it from the list so we can keep track of the ones
                # that we know are in memory
                new_story_ids.remove(story_id)
                if state == 'dead':
                    # this one was dead and now it is not
                    logger.info('%s: story resurrected', story_id)
                    yield (self._get_story_data(story_id), 'resurrected')
                elif state == 'old':
                    # benjamin button vibes
                    pass
                self.memory.put_value(story_id, 'state', 'new')
                continue

            if state != 'new':
                # we'll save these for now until they are garbage collected
                continue

            story = self._get_story_data(story_id, force=True)
            if story.get('dead'):
                # got killed
                logger.info('%s: story killed', story_id)
                yield (story, 'killed')
                self.memory.put_value(story_id, 'state', 'dead')
            elif story.get('deleted'):
                # was deleted by the user
                logger.info('%s: story deleted', story_id)
                yield (story, 'deleted')
                self.memory.put_value(story_id, 'state', 'deleted')
            else:
                # this one got too old so it does not show on the list
                # of new stories.
                logger.info('%s: story got old', story_id)
                self.memory.put_value(story_id, 'state', 'old')

        for story_id in new_story_ids:
            # the ones that weren't in memory are new
            logger.info('%s: adding new story', story_id)
            # store all data in case it's deleted later
            self._get_story_data(story_id)
            self.memory.put_value(story_id, 'state', 'new')

        for i, story_id in enumerate(self.request(self.TOP_URL)):
            # save the ranks for stories that are not in memory
            self.memory.put_list_value(story_id, 'rank', (i + 1, time()), 2)
            if rank_change := self._get_rank_change(story_id):
                story = self._get_story_data(story_id)
                if rank_change == 1:
                    # a story that was downranked before has returned to the
                    # front page. usually the result of moderator action
                    yield(story, 'dedownranked')
                elif rank_change == 2:
                    # a story that hasn't been anywhere the front page in
                    # 2 hours is now on the front page. really fishy
                    yield(story, 'upranked')
                elif rank_change == -1:
                    # a story that was on the front page has been cast away
                    yield(story, 'downranked')

        self.memory.flush_memory()
