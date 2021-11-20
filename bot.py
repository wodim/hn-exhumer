import configparser
import logging
import time

import telegram
from telegram import Update
from telegram.ext import CallbackContext, MessageHandler, Updater

from hn import HN


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


hn = HN()


def _config(k: str) -> str:
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['bot'][k]


def _e(text: str) -> str:
    """escapes text with markdown v2 syntax"""
    return telegram.utils.helpers.escape_markdown(text, 2)


def ellipsis(text: str, max_: int) -> str:
    return text[:max_ - 1] + '…' if len(text) > max_ else text


def _story_meta(story):
    if 'score' in story and story['score'] != 1:
        yield '%s points' % story['score']
    if story.get('descendants'):
        yield '%s comment%s' % (story['descendants'], 's' if story['descendants'] != 1 else '')
    elif 'kids' in story and len(story['kids']) > 0:
        yield '%s comment%s' % (len(story['kids']), 's' if len(story['kids']) != 1 else '')


def cron(context: CallbackContext) -> None:
    for story, state in hn.get_updates():
        if 'title' not in story:
            story['title'] = '<no title>'

        text = '*%s: %s*' % (state.upper(), _e(story['title']))

        if 'url' in story:
            text += '\n' + _e(story['url'])

        if (meta := list(_story_meta(story))):
            text += '\n' + _e(' - '.join(meta))

        if 'text' in story:
            text += '\n\n' + _e(ellipsis(HN.clean_text(story['text']).strip(), 3000))

        text += '\n\n' + _e(HN.get_permalink(story['id']))

        while True:
            try:
                context.bot.send_message(int(_config('cron_chat_id')), text,
                                         parse_mode=telegram.constants.PARSEMODE_MARKDOWN_V2,
                                         disable_web_page_preview=True)
                break
            except telegram.error.RetryAfter as exc:
                timeout = exc.retry_after + 1
                logger.info('Rate limit hit. Waiting %d seconds...', timeout)
                time.sleep(timeout)


def command_help(update: Update, _: CallbackContext) -> None:
    update.message.reply_text('Join %s' % _config('cron_chat_name'))


def main() -> None:
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    updater = Updater(_config('token'))

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # reply to anything that is said to me in private
    dispatcher.add_handler(MessageHandler(None, command_help))

    # add a "cron" job
    dispatcher.job_queue.run_repeating(cron, interval=3)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
