import configparser
import logging

import telegram
from telegram import Update
from telegram.ext import CallbackContext, MessageHandler, Updater

from hn import HN


logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

ITEM_PERMALINK = 'https://news.ycombinator.com/item?id=%s'

hn = HN()


def _config(k: str) -> str:
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['bot'][k]


def _e(text: str) -> str:
    """escapes text with markdown v2 syntax"""
    return telegram.utils.helpers.escape_markdown(text, 2)


def _story_meta(story):
    if 'score' in story and story['score'] != 1:
        yield '%s points' % story['score']
    if 'descendants' in story:
        yield '%s comment%s' % (story['descendants'], 's' if story['descendants'] != 1 else '')
    elif 'kids' in story and len(story['kids']) > 0:
        yield '%s comment%s' % (len(story['kids']), 's' if len(story['kids']) != 1 else '')


def post_thread(chat_id: int, context: CallbackContext) -> None:
    context.bot.send_chat_action(chat_id=chat_id, action=telegram.ChatAction.TYPING)

    for story, state in hn.get_updates():
        if state == 'killed':
            header = 'FLAGGED'
        elif state == 'resurrected':
            header = 'UNFLAGGED'
        else:
            raise ValueError('unknown new state:', state)

        text = '*%s: %s*' % (header, _e(story['title']))

        if 'url' in story:
            text += '\n' + _e(story['url'])

        if (meta := list(_story_meta(story))):
            text += '\n' + _e(' - '.join(meta))

        if 'text' in story:
            text += '\n\n' + _e(HN.clean_text(story['text']))

        text += '\n\n' + _e(ITEM_PERMALINK % story['id'])

        context.bot.send_message(chat_id, text,
                                 parse_mode=telegram.constants.PARSEMODE_MARKDOWN_V2,
                                 disable_web_page_preview=True)


def cron(context: CallbackContext) -> None:
    post_thread(int(_config('cron_chat_id')), context)


def command_help(update: Update, context: CallbackContext) -> None:
    update.message.reply_text('Join %s' % _config('cron_chat_name'))


def main() -> None:
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    updater = Updater(_config('token'))

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # reply to anything that is said to me in private
    dispatcher.add_handler(MessageHandler(None, command_help))

    # add a "cron" job that posts automatically every even hour
    dispatcher.job_queue.run_repeating(cron, interval=10)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
