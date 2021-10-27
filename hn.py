from html import unescape
import json
import logging

import requests

from memory import Memory


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class HN:
    NEW_URL = 'https://hacker-news.firebaseio.com/v0/newstories.json'
    TOP_URL = 'https://hacker-news.firebaseio.com/v0/topstories.json'
    ITEM_URL = 'https://hacker-news.firebaseio.com/v0/item/%s.json'
    PAGE_SIZE = 30

    def __init__(self):
        self.session = requests.Session()
        self.memory = Memory()

    def request(self, url):
        logger.info('requesting url: %s', url)
        data = self.session.get(url).text
        return json.loads(data)

    @staticmethod
    def clean_text(text):
        return unescape(text).replace('<p>', '\n\n')

    def _get_rank_change(self, story_id):
        rank_history = self.memory.rank_history(story_id)
        if len(rank_history) < 2:
            return 0
        if (rank_history[-2] < self.PAGE_SIZE and
                rank_history[-1] > self.PAGE_SIZE * 2):
            self.memory.put_value(story_id, 'downranked', 1)
            return -1
        if (rank_history[-1] < self.PAGE_SIZE and
                self.memory.get_value(story_id, 'downranked') == 1):
            self.memory.put_value(story_id, 'downranked', 0)
            return 1
        return 0

    def get_updates(self):
        new_story_ids = self.request(self.NEW_URL)
        top_story_ids = self.request(self.TOP_URL)

        for story_id in self.memory.get_all_ids():
            state = self.memory.get_value(story_id, 'state')
            rank = self.memory.get_value(story_id, 'rank')
            # state, rank = self.memory.get_values(story_id, 'state', 'rank')
            rank = int(rank) if rank else 0

            if story_id in new_story_ids:
                new_story_ids.remove(story_id)
                if state == 'dead':
                    # this one was dead and now it is not
                    logger.info('%s: story resurrected', story_id)
                    story = self.request(self.ITEM_URL % story_id)
                    yield (story, 'resurrected')
                elif state == 'old':
                    # benjamin button vibes
                    pass
                self.memory.put_value(story_id, 'state', 'new')
                continue

            if state != 'new':
                # we'll save these for now until they are garbage collected
                continue

            logger.info('%s: story disappeared...', story_id)
            story = self.request(self.ITEM_URL % story_id)
            if not story:
                # some stories are on the list of new stories but they
                # never get pushed to the api; the api just returns "null"
                logger.info('%s: nulled?', story_id)
                self.memory.put_value(story_id, 'state', 'old')
                continue

            if story.get('deleted'):
                # forget deleted stories
                logger.info('%s: deleted', story_id)
                self.memory.put_value(story_id, 'state', 'old')
                continue

            # find out why this story vanished
            if story.get('dead'):
                # got killed
                logger.info('%s: ...because it was killed', story_id)
                yield (story, 'killed')
                self.memory.put_value(story_id, 'state', 'dead')
            elif story.get('deleted'):
                # was deleted by the user
                logger.info('%s: ...because it was deleted', story_id)
                # yield (story, 'deleted')
                self.memory.put_value(story_id, 'state', 'deleted')
            else:
                # this one got too old so it does not show on the list
                # of new stories.
                logger.info('%s: ...because it is too old', story_id)
                self.memory.put_value(story_id, 'state', 'old')

        for story_id in new_story_ids:
            # the ones that weren't in memory are new
            logger.info('%s: adding new story', story_id)
            self.memory.put_value(story_id, 'state', 'new')

        for story_id in top_story_ids:
            # save the ranks for stories that are not in memory
            self.memory.put_value(story_id, 'rank', top_story_ids.index(story_id) + 1)
            if rank_change := self._get_rank_change(story_id):
                story = self.request(self.ITEM_URL % story_id)
                if rank_change == 1:
                    yield(story, 'upranked')
                elif rank_change == -1:
                    yield(story, 'downranked')

        self.memory.flush_memory()
