import logging
import sqlite3


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Sqlite3Memory:
    SQL_SELECT_VALUE = 'SELECT value FROM memory WHERE id = ? AND key = ? ORDER BY time DESC LIMIT 1'
    SQL_SELECT_VALUES = 'SELECT MAX(time), key, value FROM memory WHERE id = ? GROUP BY id, key'
    SQL_INSERT_VALUE = 'INSERT INTO memory (id, key, value, time) VALUES (?, ?, ?, DATETIME("now"))'
    SQL_DELETE_VALUE = 'DELETE FROM memory WHERE id = ? AND key = ?'
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
        self.connection = sqlite3.connect('memory.sqlite')
        self.cursor = self.connection.cursor()
        self.query_queue = []

    def get_value(self, story_id, key):
        logger.debug('retrieving from memory: %s->%s', story_id, key)
        if value := self.cursor.execute(self.SQL_SELECT_VALUE, (story_id, key)).fetchone():
            logger.debug('retrieved from memory:  %s->%s=%s', story_id, key, value[0])
            return value[0]
        logger.debug('memory empty:           %s->%s', story_id, key)
        return None

    def get_values(self, story_id, *keys):
        """unused because it's too slow. I would have to figure out another sql query.
        it's likely that it is the "group by" part that makes it slow"""
        logger.debug('retrieving from memory: %s->%s', story_id, keys)
        rows = self.cursor.execute(self.SQL_SELECT_VALUES, (story_id,)).fetchall()
        values = tuple(map({key: value for _, key, value in rows}.get, keys))
        logger.debug('retrieved from memory:  %s->%s=%s', story_id, keys, values)
        return values

    def put_value(self, story_id, key, value):
        logger.debug('saving to memory: %s->%s=%s', story_id, key, value)
        self.query_queue.append((story_id, key, value))

    def get_all_ids(self):
        logger.debug('getting all ids')
        return [x[0] for x in self.cursor.execute(self.SQL_SELECT_ALL_IDS).fetchall()]

    def rank_history(self, story_id):
        logger.debug('getting rank history for %s', story_id)
        return [int(x[0]) for x in self.cursor.execute(self.SQL_SELECT_RANK_HISTORY, (story_id,)).fetchall()]

    def flush_memory(self):
        self.cursor.execute(self.SQL_GC)
        self.cursor.executemany(self.SQL_DELETE_VALUE, ((x, y) for x, y, _ in self.query_queue))
        self.cursor.executemany(self.SQL_INSERT_VALUE, self.query_queue)
        self.query_queue = []
        self.connection.commit()
        self.connection.execute(self.SQL_VACUUM)
