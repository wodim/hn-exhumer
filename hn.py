from html import unescape
import json
import logging
import sqlite3

import requests


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class HN:
    NEW_URL = 'https://hacker-news.firebaseio.com/v0/newstories.json'
    TOP_URL = 'https://hacker-news.firebaseio.com/v0/topstories.json'
    ITEM_URL = 'https://hacker-news.firebaseio.com/v0/item/%s.json'
    PAGE_SIZE = 30

    SQL_SELECT_VALUE = 'SELECT value FROM memory WHERE id = ? AND key = ? ORDER BY time DESC LIMIT 1'
    SQL_SELECT_VALUES = 'SELECT MAX(time), key, value FROM memory WHERE id = ? GROUP BY id, key'
    SQL_INSERT_VALUE = 'INSERT INTO memory (id, key, value, time) VALUES (?, ?, ?, DATETIME("now"))'
    SQL_SELECT_ALL_IDS = 'SELECT DISTINCT id FROM memory WHERE time >= DATETIME("now", "-1 day")'
    SQL_SELECT_RANK_HISTORY = ("""
        SELECT value FROM memory
            WHERE time >= DATETIME("now", "-1 day")
                AND id = ?
                AND key = "rank"
            ORDER BY time ASC
    """)
    SQL_GC = 'DELETE FROM memory WHERE time < DATETIME("now", "-1 day")'
    SQL_VACUUM = 'VACUUM'

    def __init__(self):
        self.session = requests.Session()
        self.connection = sqlite3.connect('memory.sqlite')
        self.cursor = self.connection.cursor()
        self.query_queue = []

    def request(self, url):
        logger.info('requesting url: %s', url)
        data = self.session.get(url).text
        return json.loads(data)

    @staticmethod
    def clean_text(text):
        return unescape(text).replace('<p>', '\n\n')

    def _get_value(self, story_id, key):
        logger.debug('retrieving from memory: %s->%s', story_id, key)
        if value := self.cursor.execute(self.SQL_SELECT_VALUE, (story_id, key)).fetchone():
            logger.debug('retrieved from memory:  %s->%s=%s', story_id, key, value[0])
            return value[0]
        logger.debug('memory empty:           %s->%s', story_id, key)
        return None

    def _get_values(self, story_id, *keys):
        """unused because it's too slow. I would have to figure out another sql query.
        it's likely that it is the "group by" part that makes it slow"""
        logger.debug('retrieving from memory: %s->%s', story_id, keys)
        rows = self.cursor.execute(self.SQL_SELECT_VALUES, (story_id,)).fetchall()
        values = tuple(map({key: value for _, key, value in rows}.get, keys))
        logger.debug('retrieved from memory:  %s->%s=%s', story_id, keys, values)
        return values

    def _put_value(self, story_id, key, value):
        logger.debug('saving to memory: %s->%s=%s', story_id, key, value)
        self.query_queue.append((story_id, key, value))

    def _get_all_ids(self):
        logger.debug('getting all ids')
        return [x[0] for x in self.cursor.execute(self.SQL_SELECT_ALL_IDS).fetchall()]

    def _rank_history(self, story_id):
        logger.debug('getting rank history for %s', story_id)
        return [int(x[0]) for x in self.cursor.execute(self.SQL_SELECT_RANK_HISTORY, (story_id,)).fetchall()]

    def _get_rank_change(self, story_id):
        rank_history = self._rank_history(story_id)
        if len(rank_history) < 2:
            return 0
        if (rank_history[-2] < self.PAGE_SIZE and
                rank_history[-1] > self.PAGE_SIZE * 2):
            self._put_value(story_id, 'downranked', 1)
            return -1
        if (rank_history[-1] < self.PAGE_SIZE and
                self._get_value(story_id, 'downranked') == 1):
            self._put_value(story_id, 'downranked', 0)
            return 1
        return 0

    def _flush_memory(self):
        self.cursor.execute(self.SQL_GC)
        self.cursor.executemany(self.SQL_INSERT_VALUE, self.query_queue)
        self.query_queue = []
        self.connection.commit()
        self.connection.execute(self.SQL_VACUUM)

    def get_updates(self):
        new_story_ids = self.request(self.NEW_URL)
        top_story_ids = self.request(self.TOP_URL)

        for story_id in self._get_all_ids():
            state = self._get_value(story_id, 'state')
            rank = self._get_value(story_id, 'rank')
            # state, rank = self._get_values(story_id, 'state', 'rank')
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
                self._put_value(story_id, 'state', 'new')
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
                self._put_value(story_id, 'state', 'old')
                continue

            if story.get('deleted'):
                # forget deleted stories
                logger.info('%s: deleted', story_id)
                self._put_value(story_id, 'state', 'old')
                continue

            # find out why this story vanished
            if story.get('dead'):
                # got killed
                logger.info('%s: ...because it was killed', story_id)
                yield (story, 'killed')
                self._put_value(story_id, 'state', 'dead')
            elif story.get('deleted'):
                # was deleted by the user
                logger.info('%s: ...because it was deleted', story_id)
                # yield (story, 'deleted')
                self._put_value(story_id, 'state', 'deleted')
            else:
                # this one got too old so it does not show on the list
                # of new stories.
                logger.info('%s: ...because it is too old', story_id)
                self._put_value(story_id, 'state', 'old')

        for story_id in new_story_ids:
            # the ones that weren't in memory are new
            logger.info('%s: adding new story', story_id)
            self._put_value(story_id, 'state', 'new')

        for story_id in top_story_ids:
            # save the ranks for stories that are not in memory
            self._put_value(story_id, 'rank', top_story_ids.index(story_id) + 1)
            if rank_change := self._get_rank_change(story_id):
                story = self.request(self.ITEM_URL % story_id)
                if rank_change == 1:
                    yield(story, 'upranked')
                elif rank_change == -1:
                    yield(story, 'downranked')

        self._flush_memory()
