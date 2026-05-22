# coding=utf-8

import html
import itertools
import logging
import os
import tempfile
import threading
import traceback
import urllib.parse
from collections import defaultdict
from pathlib import Path
from typing import Tuple, Optional, TYPE_CHECKING, List, IO, Union

import humanize
import pydub
import telegram  # lgtm [py/import-and-import-from]
import telegram.constants
import telegram.error
import telegram.ext
from PIL import Image
from telegram import InputFile, ChatAction, InputMediaAudio, InputMediaPhoto, InputMediaDocument, InputMediaVideo, InputMediaAnimation, \
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyMarkup, TelegramError, InputMedia

from ehforwarderbot import Message, Status, coordinator
from ehforwarderbot.chat import ChatNotificationState, SelfChatMember, GroupChat, PrivateChat, SystemChat, Chat
from ehforwarderbot.constants import MsgType
from ehforwarderbot.message import LinkAttribute, LocationAttribute, MessageCommand, Reactions, \
    StatusAttribute
from ehforwarderbot.status import ChatUpdates, MemberUpdates, MessageRemoval, MessageReactionsUpdate
from . import utils
from .chat_destination_cache import ChatDestinationCache
from .chat_object_cache import ChatObjectCacheManager
from .commands import ETMCommandMsgStorage
from .constants import Emoji
from .locale_mixin import LocaleMixin
from .message import ETMMsg
from .msg_type import get_msg_type
from .utils import TelegramChatID, TelegramTopicID, TelegramMessageID, OldMsgID

if TYPE_CHECKING:
    from . import TelegramChannel
    from .bot_manager import TelegramBotManager
    from .db import DatabaseManager


class SlaveMessageProcessor(LocaleMixin):
    """Process messages as Message objects from slave channels."""

    def __init__(self, channel: 'TelegramChannel'):
        self.channel: 'TelegramChannel' = channel
        self.bot: 'TelegramBotManager' = self.channel.bot_manager
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.flag: utils.ExperimentalFlagsManager = self.channel.flag
        self.db: 'DatabaseManager' = channel.db
        self.chat_dest_cache: ChatDestinationCache = channel.chat_dest_cache
        self.chat_manager: ChatObjectCacheManager = channel.chat_manager

    def is_silent(self, msg: Message) -> Optional[bool]:
        """Determine if a message shall be sent silently.
        Returns None if the message shall not be sent at all.
        """
        xid = msg.uid
        if isinstance(msg.author, SelfChatMember):
            # Message is send by admin not through EFB
            your_slave_msg = self.flag('your_message_on_slave')
            if your_slave_msg == 'silent':
                return True
            elif your_slave_msg == 'mute':
                self.logger.debug("[%s] Message is muted as it is from the admin.", xid)
                return None
        elif msg.chat.notification == ChatNotificationState.NONE or \
                (msg.chat.notification == ChatNotificationState.MENTIONS and
                 (not msg.substitutions or not msg.substitutions.is_mentioned)):
            # Shall not be notified in slave channel
            muted_on_slave = self.flag('message_muted_on_slave')
            if muted_on_slave == 'silent':
                return True
            elif muted_on_slave == 'mute':
                self.logger.debug("[%s] Message is muted due to slave channel settings.", xid)
                return None
        return False

    def send_message(self, msg: Message) -> Message:
        """
        Process a message from slave channel and deliver it to the user.

        Args:
            msg (Message): The message.
        """
        try:
            xid = msg.uid
            self.logger.debug("[%s] Slave message delivered to ETM.\n%s", xid, msg)

            msg_template, (tg_dest, thread_id) = self.get_slave_msg_dest(msg)

            silent = self.is_silent(msg)
            if silent is None:
                self.logger.debug("[%s] Message is not delivered per silent settings.", xid)
                return msg

            if tg_dest is None:
                self.logger.debug("[%s] Sender of the message is muted.", xid)
                return msg

            # When editing message
            old_msg_id: Optional[OldMsgID] = None
            if msg.edit:
                old_msg = self.db.get_msg_log(slave_msg_id=msg.uid,
                                              slave_origin_uid=utils.chat_id_to_str(chat=msg.chat))
                if old_msg:

                    if old_msg.master_msg_id_alt:
                        old_msg_id = utils.message_id_str_to_id(old_msg.master_msg_id_alt)
                    else:
                        old_msg_id = utils.message_id_str_to_id(old_msg.master_msg_id)
                else:
                    self.logger.info('[%s] Was supposed to edit this message, '
                                     'but it does not exist in database. Sending new message instead.',
                                     msg.uid)

            self.dispatch_message(msg=msg, msg_template=msg_template, old_msg_id=old_msg_id, tg_dest=tg_dest, thread_id=thread_id, silent=silent)
        except Exception as e:
            self.logger.error("Error occurred while processing message from slave channel.\nMessage: %s\n%s\n%s",
                              repr(msg), repr(e), traceback.format_exc())
        return msg

    @staticmethod
    def handle_topic_error(fn):
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except telegram.error.BadRequest as e:
                if "Message thread not found" in e.message:
                    self = args[0]
                    self.logger.warning("Message thread not found, removing binding and retrying.")
                    message_thread_id = kwargs.pop('thread_id')
                    tg_dest = kwargs.get('tg_dest')
                    msg = kwargs.get('msg')

                    self.db.remove_topic_assoc(topic_chat_id=tg_dest, message_thread_id=message_thread_id)
                    if msg.file and getattr(msg.file, 'closed', False) and msg.path:
                        msg.file = open(msg.path, 'rb')
                    return fn(*args, msg=msg, tg_dest=tg_dest, message_thread_id=message_thread_id, **kwargs)
                else:
                    raise e
        return wrapper

    @handle_topic_error
    def dispatch_message(self, msg: Message, msg_template: str,
                         old_msg_id: Optional[OldMsgID],
                         tg_dest: TelegramChatID,
                         thread_id: Optional[TelegramTopicID],
                         silent: bool = False):
        """Dispatch with header, destination and Telegram message ID and destinations."""

        xid = msg.uid

        # When targeting a message (reply to)
        target_msg_id: Optional[TelegramMessageID] = None
        target_quote_text: Optional[str] = None
        if isinstance(msg.target, Message):
            self.logger.debug("[%s] Message is replying to %s.", msg.uid, msg.target)
            log = self.db.get_msg_log(
                slave_msg_id=msg.target.uid,
                slave_origin_uid=utils.chat_id_to_str(chat=msg.target.chat)
            )
            if not log:
                self.logger.debug("[%s] Target message %s is not found in database.", msg.uid, msg.target)
            else:
                self.logger.debug("[%s] Target message has database entry: %s.", msg.uid, log)
                target_msg = utils.message_id_str_to_id(log.master_msg_id)
                # Assuming target_msg = (chat_id, message_id). Thread ID might need separate handling/DB storage.
                # We only check if the reply target is in the same main chat. Replying across topics is allowed by Telegram.
                if not target_msg or target_msg[0] != int(tg_dest):
                    self.logger.error('[%s] Trying to reply to a message not from this chat. '
                                      'Message destination: %s. Target message: %s.',
                                      msg.uid, tg_dest, target_msg)
                    target_msg_id = None
                else:
                    target_msg_id = target_msg[1]
                    target_quote_text = log.text

        # Generate basic reply markup
        commands: Optional[List[MessageCommand]] = None
        reply_markup: Optional[InlineKeyboardMarkup] = None

        if msg.commands:
            commands = msg.commands
            buttons = []
            for idx, i in enumerate(commands):
                buttons.append([InlineKeyboardButton(i.name, callback_data=str(idx))])
            reply_markup = InlineKeyboardMarkup(buttons)

        reactions = self.build_reactions_footer(msg.reactions)

        msg.text = msg.text or ""

        # Type dispatching
        if msg.type == MsgType.Text:
            tg_msg = self.slave_message_text(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                             target_quote_text,
                                             reply_markup, silent)
        elif msg.type == MsgType.Link:
            tg_msg = self.slave_message_link(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                             target_quote_text,
                                             reply_markup, silent)
        elif msg.type == MsgType.Sticker:
            tg_msg = self.slave_message_sticker(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                                target_quote_text,
                                                reply_markup, silent)
        elif msg.type == MsgType.Image:
            if self.flag("send_image_as_file"):
                tg_msg = self.slave_message_file(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                                 target_quote_text,
                                                 reply_markup, silent)
            else:
                tg_msg = self.slave_message_image(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                                  target_quote_text,
                                                  reply_markup, silent)
        elif msg.type == MsgType.Animation:
            tg_msg = self.slave_message_animation(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                                  target_quote_text,
                                                  reply_markup, silent)
        elif msg.type == MsgType.File:
            tg_msg = self.slave_message_file(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                             target_quote_text,
                                             reply_markup, silent)
        elif msg.type == MsgType.Voice:
            tg_msg = self.slave_message_voice(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                              target_quote_text,
                                              reply_markup, silent)
        elif msg.type == MsgType.Location:
            tg_msg = self.slave_message_location(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                                 target_quote_text,
                                                 reply_markup, silent)
        elif msg.type == MsgType.Video:
            tg_msg = self.slave_message_video(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id, target_msg_id,
                                              target_quote_text,
                                              reply_markup, silent)
        elif msg.type == MsgType.Status:
            # Status messages are not to be recorded in databases
            return self.slave_message_status(msg, tg_dest, thread_id)
        elif msg.type == MsgType.Unsupported:
            tg_msg = self.slave_message_unsupported(msg, tg_dest, thread_id, msg_template, reactions, old_msg_id,
                                                    target_msg_id, target_quote_text, reply_markup, silent)
        else:
            self.bot.send_chat_action(tg_dest, ChatAction.TYPING, message_thread_id=thread_id)
            tg_msg = self.bot.send_message(
                tg_dest,
                prefix=self.format_text_message_template(msg_template, msg.author),
                suffix=reactions,
                parse_mode="HTML",
                reply_markup=reply_markup,
                message_thread_id=thread_id,
                disable_notification=silent,
                text=self._('Unknown type of message "{0}". (UT01)').format(msg.type.name),
                **self.build_reply_target_kwargs(target_msg_id, target_quote_text)
            )

        if tg_msg and commands:
            self.channel.commands.register_command(tg_msg, ETMCommandMsgStorage(
                commands, coordinator.get_module_by_id(msg.author.module_id), msg_template, msg.text
            ))

        self.logger.debug("[%s] Message is sent to the user with telegram message id %s.%s.",
                          xid, tg_msg.chat.id, tg_msg.message_id)

        etm_msg = ETMMsg.from_efbmsg(msg, self.chat_manager)
        etm_msg.type_telegram = get_msg_type(tg_msg)
        etm_msg.put_telegram_file(tg_msg)
        self.db.add_or_update_message_log(etm_msg, tg_msg, old_msg_id)
        # self.logger.debug("[%s] Message inserted/updated to the database.", xid)

    def get_slave_msg_dest(self, msg: Message) -> Tuple[str, Tuple[Optional[TelegramChatID], Optional[TelegramTopicID]]]:
        """Get the Telegram destination of a message with its header.

        Returns:
            msg_template (str): header of the message.
            (Optional[TelegramChatID], Optional[TelegramTopicID]): Telegram destination chat ID and thread ID, None if muted.
        """
        xid = msg.uid
        chat = self.chat_manager.update_chat_obj(msg.chat)
        msg.chat = chat
        msg.author = self.chat_manager.get_or_enrol_member(msg.chat, msg.author)

        chat_uid = utils.chat_id_to_str(chat=msg.chat)
        tg_chats = self.db.get_chat_assoc(slave_uid=chat_uid)
        tg_chat = None
        tg_dest: Optional[TelegramChatID] = None
        thread_id: Optional[TelegramTopicID] = None

        if tg_chats:
            tg_chat = tg_chats[0]
        self.logger.debug("[%s] The message should deliver to %s", xid, tg_chat)

        singly_linked = True
        if tg_chat:
            slaves = self.db.get_chat_assoc(master_uid=tg_chat)
            if slaves and len(slaves) > 1:
                singly_linked = False
                self.logger.debug("[%s] Sender is linked with other chats in a Telegram group.", xid)
        self.logger.debug("[%s] Message is in chat %s", xid, msg.chat)

        # Generate chat text template & Decide type target
        tg_dest = TelegramChatID(self.channel.config['admins'][0])
        
        if tg_chat:
            tg_dest = TelegramChatID(int(utils.chat_id_str_to_id(tg_chat)[1]))
        if self.channel.topic_group:
            if not isinstance(chat, SystemChat):
                tg_dest = TelegramChatID(int(utils.chat_id_str_to_id(tg_chat)[1]) if tg_chat else self.channel.topic_group)
                master_chat_info = self.bot.get_chat_info(tg_dest)
                if master_chat_info.is_forum:
                    thread_id = self.channel.chat_binding.create_topic(slave_uid=chat_uid, telegram_chat_id=tg_dest)

        if not tg_chat:
            singly_linked = False
        if thread_id:
            singly_linked = True

        msg_template = self.generate_message_template(msg, singly_linked)
        self.logger.debug("[%s] Message is sent to Telegram chat %s, with header \"%s\".",
                          xid, tg_dest, msg_template)

        if self.chat_dest_cache.get(str(tg_dest)) != chat_uid:
            self.chat_dest_cache.remove(str(tg_dest))

        return msg_template, (tg_dest, thread_id)


    def html_substitutions(self, msg: Message) -> str:
        """Build a Telegram-flavored HTML string for message text substitutions."""
        text = msg.text
        if msg.substitutions:
            ranges = sorted(msg.substitutions.keys())
            t = ""
            prev = 0
            for i in ranges:
                t += html.escape(text[prev:i[0]])
                sub_chat = msg.substitutions[i]
                if isinstance(sub_chat, SelfChatMember) or (isinstance(sub_chat, Chat) and sub_chat.has_self):
                    t += f'<a href="tg://user?id={self.channel.config["admins"][0]}">'
                    t += html.escape(text[i[0]:i[1]])
                    t += "</a>"
                else:
                    t += '<code>'
                    t += html.escape(text[i[0]:i[1]])
                    t += '</code>'
                prev = i[1]
            t += html.escape(text[prev:])
            return t
        elif text:
            return html.escape(text)
        return text

    def slave_message_text(self, msg: Message, tg_dest: TelegramChatID,
                           thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                           old_msg_id: OldMsgID = None,
                           target_msg_id: Optional[TelegramMessageID] = None,
                           target_quote_text: Optional[str] = None,
                           reply_markup: Optional[ReplyMarkup] = None,
                           silent: bool = False) -> telegram.Message:
        """
        Send message as text to Telegram.

        Args:
            msg (Message): Message
            tg_dest (TelegramChatID): Telegram Chat ID
            thread_id (Optional[TelegramTopicID]): Telegram Thread ID
            msg_template: Header of the message
            reactions: Footer of the message
            old_msg_id: Telegram message ID to edit
            target_msg_id: Telegram message ID to reply to
            reply_markup: Reply markup to be added to the message
            silent: Silent notification of the message when sending
        Returns:
            The telegram bot message object sent
        """
        self.logger.debug("[%s] Sending as a text message.", msg.uid)
        self.bot.send_chat_action(tg_dest, ChatAction.TYPING, message_thread_id=thread_id)

        text = self.html_substitutions(msg)
        text_prefix = self.format_text_message_template(msg_template, msg.author)

        if not old_msg_id:
            tg_msg = self.bot.send_message(tg_dest,
                                           text=text, prefix=text_prefix, suffix=reactions,
                                           parse_mode='HTML',
                                           message_thread_id=thread_id,
                                           reply_markup=reply_markup,
                                           disable_notification=silent,
                                           **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
        else:
            # Cannot change reply_to_message_id when editing a message
            tg_msg = self.bot.edit_message_text(chat_id=old_msg_id[0],
                                                message_id=old_msg_id[1],
                                                text=text, prefix=text_prefix, suffix=reactions,
                                                parse_mode='HTML',
                                                reply_markup=reply_markup)

        self.logger.debug("[%s] Processed and sent as text message", msg.uid)
        return tg_msg

    def slave_message_link(self, msg: Message, tg_dest: TelegramChatID,
                           thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                           old_msg_id: OldMsgID = None,
                           target_msg_id: Optional[TelegramMessageID] = None,
                           target_quote_text: Optional[str] = None,
                           reply_markup: Optional[ReplyMarkup] = None,
                           silent: bool = False) -> telegram.Message:
        self.bot.send_chat_action(tg_dest, ChatAction.TYPING, message_thread_id=thread_id)
        text_prefix = self.format_text_message_template(msg_template, msg.author)

        assert isinstance(msg.attributes, LinkAttribute)
        attributes: LinkAttribute = msg.attributes

        thumbnail = urllib.parse.quote(attributes.image or "", safe="?=&#:/")
        thumbnail = "<a href=\"%s\">🔗</a>" % thumbnail if thumbnail else "🔗"
        text = "%s <a href=\"%s\">%s</a>\n%s" % \
               (thumbnail,
                urllib.parse.quote(attributes.url, safe="?=&#:/"),
                html.escape(attributes.title or attributes.url),
                html.escape(attributes.description or ""))

        if msg.text:
            text += "\n\n" + self.html_substitutions(msg)
        if old_msg_id:
            return self.bot.edit_message_text(text=text, chat_id=old_msg_id[0], message_id=old_msg_id[1],
                                              prefix=text_prefix, suffix=reactions, parse_mode='HTML',
                                              reply_markup=reply_markup)
        else:
            return self.bot.send_message(chat_id=tg_dest,
                                         text=text,
                                         prefix=text_prefix, suffix=reactions,
                                         parse_mode="HTML",
                                         message_thread_id=thread_id,
                                         reply_markup=reply_markup,
                                         disable_notification=silent,
                                         **self.build_reply_target_kwargs(target_msg_id, target_quote_text))

    # Parameters to decide when to pictures as files
    IMG_MIN_SIZE = 1600
    """Threshold of dimension of the shorter side to send as file."""
    IMG_MAX_SIZE = 1200
    """Threshold of dimension of the longer side to send as file, used along with IMG_SIZE_RATIO."""
    IMG_SIZE_RATIO = 3.5
    """Threshold of aspect ratio (longer side to shorter side) to send as file, used along with IMG_SIZE_RATIO."""
    IMG_SIZE_MAX_RATIO = 10
    """Threshold of aspect ratio (longer side to shorter side) to send as file, used alone."""

    def slave_message_image(self, msg: Message, tg_dest: TelegramChatID,
                            thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                            old_msg_id: OldMsgID = None,
                            target_msg_id: Optional[TelegramMessageID] = None,
                            target_quote_text: Optional[str] = None,
                            reply_markup: Optional[ReplyMarkup] = None,
                            silent: bool = False) -> telegram.Message:
        assert msg.file
        self.bot.send_chat_action(tg_dest, ChatAction.UPLOAD_PHOTO, message_thread_id=thread_id)
        self.logger.debug("[%s] Message is of %s type; Path: %s; MIME: %s", msg.uid, msg.type, msg.path, msg.mime)
        if msg.path:
            self.logger.debug("[%s] Size of %s is %s.", msg.uid, msg.path, os.stat(msg.path).st_size)

        if msg.text:
            text = self.html_substitutions(msg)
        elif msg_template:
            placeholder_flag = self.flag("default_media_prompt")
            if placeholder_flag == "emoji":
                text = "🖼️"
            elif placeholder_flag == "text":
                text = self._("Sent a picture.")
            else:
                text = ""
        else:
            text = ""
        text_prefix = self.format_text_message_template(msg_template, msg.author)
        try:
            # Avoid Telegram compression of pictures by sending high definition image messages as files
            # Code adopted from wolfsilver's fork:
            # https://github.com/wolfsilver/efb-telegram-master/blob/99668b60f7ff7b6363dfc87751a18281d9a74a09/efb_telegram_master/slave_message.py#L142-L163
            #
            # Rules:
            # 1. If the picture is too large -- shorter side is greater than IMG_MIN_SIZE, send as file.
            # 2. If the picture is large and thin --
            #        longer side is greater than IMG_MAX_SIZE, and
            #        aspect ratio (longer to shorter side ratio) is greater than IMG_SIZE_RATIO,
            #    send as file.
            # 3. If the picture is too thin -- aspect ratio grater than IMG_SIZE_MAX_RATIO, send as file.

            try:
                pic_img = Image.open(msg.path)
                max_size = max(pic_img.size)
                min_size = min(pic_img.size)
                img_ratio = max_size / min_size

                if min_size > self.IMG_MIN_SIZE:
                    send_as_file = True
                elif max_size > self.IMG_MAX_SIZE and img_ratio > self.IMG_SIZE_RATIO:
                    send_as_file = True
                elif img_ratio >= self.IMG_SIZE_MAX_RATIO:
                    send_as_file = True
                else:
                    send_as_file = False
            except IOError:  # Ignore when the image cannot be properly identified.
                send_as_file = False

            file_too_large = self.check_file_size(msg.file)
            edit_media = msg.edit_media
            if file_too_large:
                if old_msg_id:
                    if msg.edit_media:
                        edit_media = False
                    self.bot.send_message(chat_id=old_msg_id[0], reply_to_message_id=old_msg_id[1], text=file_too_large)
                else:
                    message = self.bot.send_message(chat_id=tg_dest,
                                                    message_thread_id=thread_id, text=text,
                                                    parse_mode="HTML", reply_markup=reply_markup, disable_notification=silent,
                                                    prefix=text_prefix, suffix=reactions,
                                                    **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
                    message.reply_text(file_too_large)
                    return message

            reusable_file_id: Optional[str] = None
            if msg.path and (not old_msg_id) and (not msg.edit_media):
                media_type = "document" if send_as_file else "photo"
                reusable_file_id = self.channel.image_dedupe.try_find_similar_file_id(msg.path, tg_media_type=media_type)
                if reusable_file_id:
                    self.logger.debug("[%s] Found similar %s in Telegram cloud, reuse file_id.", msg.uid, media_type)

            if old_msg_id:
                try:
                    if edit_media:
                        assert msg.path
                        media: InputMedia
                        file = self.process_file_obj(msg.file, msg.path)
                        if send_as_file:
                            media = InputMediaDocument(file, filename=msg.filename)
                        else:
                            media = InputMediaPhoto(file, filename=msg.filename)
                        return self.bot.edit_message_media(chat_id=old_msg_id[0], message_id=old_msg_id[1], media=media, reply_markup=reply_markup,
                                                         prefix=text_prefix, suffix=reactions, caption=text, parse_mode="HTML")
                    return self.bot.edit_message_caption(chat_id=old_msg_id[0], message_id=old_msg_id[1],
                                                         reply_markup=reply_markup,
                                                         prefix=text_prefix, suffix=reactions, caption=text, parse_mode="HTML")
                except telegram.error.BadRequest as e:
                    self.logger.warning("[%s] Failed to edit media/caption (BadRequest: %s). Sending new message instead.", msg.uid, e)
                    # Send as a reply if cannot edit previous message.
                    # Check if the target is within the same chat_id (thread_id doesn't matter for this check)
                    if old_msg_id[0] == str(tg_dest):
                        target_msg_id = target_msg_id or old_msg_id[1] # Reply to the original message
                    msg.file.seek(0)
                    # Fall through to send a new message

            def _send_document(file_or_id) -> telegram.Message:
                tg_msg = self.bot.send_document(
                    tg_dest,
                    file_or_id,
                    prefix=text_prefix,
                    suffix=reactions,
                    caption=text,
                    parse_mode="HTML",
                    filename=msg.filename,
                    message_thread_id=thread_id,
                    reply_markup=reply_markup,
                    disable_notification=silent,
                    **self.build_reply_target_kwargs(target_msg_id, target_quote_text),
                )

                doc = tg_msg.document if getattr(tg_msg, "document", None) else None
                self.channel.image_dedupe.enqueue_index(
                    msg.path,
                    doc.file_id if doc else None,
                    doc.file_unique_id if doc else None,
                    tg_media_type="document",
                    mime=msg.mime,
                )
                return tg_msg

            file_or_id = reusable_file_id or self.process_file_obj(msg.file, msg.path)
            # Sending new message (either initially or as fallback from edit)
            if send_as_file:
                assert msg.path
                return _send_document(
                    file_or_id=file_or_id,
                )
            else:
                try:
                    assert msg.path
                    tg_msg = self.bot.send_photo(tg_dest, file_or_id, prefix=text_prefix, suffix=reactions,
                                                    caption=text, parse_mode="HTML",
                                                    message_thread_id=thread_id,
                                                    reply_markup=reply_markup,
                                                    disable_notification=silent,
                                                    **self.build_reply_target_kwargs(target_msg_id, target_quote_text))

                    photo = tg_msg.photo[-1] if getattr(tg_msg, "photo", None) else None
                    self.channel.image_dedupe.enqueue_index(
                        msg.path,
                        photo.file_id if photo else None,
                        photo.file_unique_id if photo else None,
                        tg_media_type="photo",
                        mime=msg.mime,
                    )
                    return tg_msg
                except telegram.error.BadRequest as e:
                    self.logger.error('[%s] Failed to send it as image, sending as document. Reason: %s',
                                      msg.uid, e)
                    assert msg.path
                    msg.file.seek(0) # Rewind file pointer
                    file = self.process_file_obj(msg.file, msg.path)
                    return _send_document(
                        file_or_id=file,
                    )
        finally:
            if msg.file:
                msg.file.close()

    def slave_message_animation(self, msg: Message, tg_dest: TelegramChatID,
                                thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                                old_msg_id: OldMsgID = None,
                                target_msg_id: Optional[TelegramMessageID] = None,
                                target_quote_text: Optional[str] = None,
                                reply_markup: Optional[ReplyMarkup] = None,
                                silent: bool = None) -> telegram.Message:
        self.bot.send_chat_action(tg_dest, ChatAction.UPLOAD_PHOTO, message_thread_id=thread_id) # UPLOAD_VIDEO_NOTE might be better?

        self.logger.debug("[%s] Message is an Animation; Path: %s; MIME: %s", msg.uid, msg.path, msg.mime)
        if msg.path:
            self.logger.debug("[%s] Size of %s is %s.", msg.uid, msg.path, os.stat(msg.path).st_size)

        if msg.text:
            text = self.html_substitutions(msg)
        else:
            text = ""
        text_prefix = self.format_text_message_template(msg_template, msg.author)

        try:
            file_too_large = self.check_file_size(msg.file)
            edit_media = msg.edit_media
            if file_too_large:
                if old_msg_id:
                    if msg.edit_media:
                        edit_media = False
                    self.bot.send_message(chat_id=old_msg_id[0], reply_to_message_id=old_msg_id[1], text=file_too_large)
                else:
                    message = self.bot.send_message(chat_id=tg_dest,
                                                    message_thread_id=thread_id, text=text,
                                                    parse_mode="HTML", reply_markup=reply_markup,
                                                    disable_notification=silent,
                                                    prefix=text_prefix, suffix=reactions,
                                                    **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
                    message.reply_text(file_too_large)
                    return message

            reusable_file_id: Optional[str] = None
            if msg.path and (not old_msg_id) and (not msg.edit_media):
                reusable_file_id = self.channel.image_dedupe.try_find_similar_file_id(msg.path, tg_media_type="animation")
                if reusable_file_id:
                    self.logger.debug("[%s] Found similar animation in Telegram cloud, reuse file_id.", msg.uid)

            if old_msg_id:
                if edit_media:
                    assert msg.file and msg.path
                    file = self.process_file_obj(msg.file, msg.path)
                    return self.bot.edit_message_media(chat_id=old_msg_id[0], message_id=old_msg_id[1], media=InputMediaAnimation(file, filename=msg.filename), reply_markup=reply_markup,
                                                            prefix=text_prefix, suffix=reactions, caption=text, parse_mode="HTML")
                return self.bot.edit_message_caption(chat_id=old_msg_id[0], message_id=old_msg_id[1],
                                                     prefix=text_prefix, suffix=reactions,
                                                     reply_markup=reply_markup,
                                                     caption=text, parse_mode="HTML")
            else:
                assert msg.file and msg.path
                if reusable_file_id:
                    animation = reusable_file_id 
                else:
                    file = self.process_file_obj(msg.file, msg.path)
                    animation: Union[IO[bytes], bytes] = open(utils.coerce_local_path(file), 'rb') if isinstance(file, str) else file
                if not isinstance(animation, str):
                    animation = InputFile(animation, filename=msg.filename)

                tg_msg = self.bot.send_animation(
                    tg_dest,
                    animation,
                    prefix=text_prefix,
                    suffix=reactions,
                    caption=text,
                    parse_mode="HTML",
                    message_thread_id=thread_id,
                    reply_markup=reply_markup,
                    disable_notification=silent,
                    **self.build_reply_target_kwargs(target_msg_id, target_quote_text),
                )

                ani = tg_msg.animation if getattr(tg_msg, "animation", None) else None
                self.channel.image_dedupe.enqueue_index(
                    msg.path,
                    ani.file_id if ani else None,
                    ani.file_unique_id if ani else None,
                    tg_media_type="animation",
                    mime=msg.mime,
                )
                return tg_msg
        finally:
            if msg.file is not None:
                msg.file.close()

    def slave_message_sticker(self, msg: Message, tg_dest: TelegramChatID,
                              thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                              old_msg_id: OldMsgID = None,
                              target_msg_id: Optional[TelegramMessageID] = None,
                              target_quote_text: Optional[str] = None,
                              reply_markup: Optional[InlineKeyboardMarkup] = None,
                              silent: bool = False) -> telegram.Message:

        self.bot.send_chat_action(tg_dest, ChatAction.UPLOAD_PHOTO, message_thread_id=thread_id)

        sticker_reply_markup = self.build_chat_info_inline_keyboard(msg, msg_template, reactions, reply_markup)
        text_prefix = self.format_text_message_template(msg_template, msg.author)

        self.logger.debug("[%s] Message is of %s type; Path: %s; MIME: %s", msg.uid, msg.type, msg.path, msg.mime)
        if msg.path:
            self.logger.debug("[%s] Size of %s is %s.", msg.uid, msg.path, os.stat(msg.path).st_size)

        try:
            reusable_file_id: Optional[str] = None
            if msg.path and (not old_msg_id) and (not msg.edit_media):
                reusable_file_id = self.channel.image_dedupe.try_find_similar_file_id(msg.path, tg_media_type="sticker")
                if reusable_file_id:
                    self.logger.debug("[%s] Found similar sticker in Telegram cloud, reuse file_id.", msg.uid)

            # If only media changed (e.g., replaced sticker), send new one replying to old.
            # Telegram doesn't support editing sticker media directly.
            if msg.edit_media and old_msg_id is not None:
                 if old_msg_id[0] == str(tg_dest):
                    target_msg_id = old_msg_id[1] # Set reply target to the message being "edited"
                 old_msg_id = None # Force sending a new message

            # If not editing media, but have old_msg_id, try editing reply_markup (e.g., for reactions)
            if old_msg_id and not msg.edit_media:
                try:
                    # Editing reply markup doesn't involve thread_id
                    return self.bot.edit_message_reply_markup(chat_id=old_msg_id[0], message_id=old_msg_id[1],
                                                              reply_markup=sticker_reply_markup)
                except TelegramError:
                    return self.bot.send_message(chat_id=old_msg_id[0], reply_to_message_id=old_msg_id[1],
                                                 prefix=text_prefix, text=msg.text, suffix=reactions,
                                                 reply_markup=reply_markup,
                                                 disable_notification=silent)

            # Sending a new sticker (initial send or edit_media fallback)
            else:
                webp_img = None

                file_too_large = self.check_file_size(msg.file)
                if file_too_large:
                    if old_msg_id:
                        self.bot.send_message(chat_id=old_msg_id[0], reply_to_message_id=old_msg_id[1],
                                              text=file_too_large)
                    else:
                        # Send placeholder text first
                        message = self.bot.send_message(chat_id=tg_dest,
                                                        message_thread_id=thread_id,
                                                        text=self.html_substitutions(msg),
                                                        parse_mode="HTML", reply_markup=reply_markup,
                                                        disable_notification=silent,
                                                        prefix=text_prefix, suffix=reactions,
                                                        **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
                        message.reply_text(file_too_large)
                        return message

                try:
                    if not reusable_file_id:
                        pic_img: Image = Image.open(msg.file)
                        webp_img = tempfile.NamedTemporaryFile(suffix='.webp')
                        pic_img.convert("RGBA").save(webp_img, 'webp')
                        webp_img.seek(0)
                        file = self.process_file_obj(webp_img, webp_img.name)
                    file_or_id = reusable_file_id or file
                    tg_msg = self.bot.send_sticker(
                        tg_dest,
                        file_or_id,
                        reply_markup=sticker_reply_markup,
                        message_thread_id=thread_id,
                        disable_notification=silent,
                        **self.build_reply_target_kwargs(target_msg_id, target_quote_text),
                    )
                    st = tg_msg.sticker if getattr(tg_msg, "sticker", None) else None
                    self.channel.image_dedupe.enqueue_index(
                        msg.path,
                        st.file_id if st else None,
                        st.file_unique_id if st else None,
                        tg_media_type="sticker",
                        mime=msg.mime,
                    )
                    return tg_msg
                except IOError:
                    self.logger.warning("[%s] Failed to convert image to webp sticker, sending as document.", msg.uid)
                    assert msg.file and msg.path
                    file = self.process_file_obj(msg.file, msg.path)
                    return self.bot.send_document(tg_dest, file, prefix=text_prefix, suffix=reactions,
                                                  message_thread_id=thread_id,
                                                  caption=msg.text, filename=msg.filename,
                                                  reply_markup=reply_markup,
                                                  disable_notification=silent,
                                                  **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
                finally:
                    if webp_img and not webp_img.closed:
                        webp_img.close()
        finally:
            if msg.file and not msg.file.closed:
                msg.file.close()

    @staticmethod
    def build_chat_info_inline_keyboard(msg: Message, msg_template: str, reactions: str,
                                        reply_markup: Optional[InlineKeyboardMarkup]
                                        ) -> InlineKeyboardMarkup:
        """
        Build inline keyboard markup with message header and footer (reactions). Buttons are attached
        before any other commands attached.
        """
        description = []
        if msg_template:
            description.append([InlineKeyboardButton(msg_template, callback_data="void")])
        if msg.text:
            description.append([InlineKeyboardButton(msg.text, callback_data="void")])
        if reactions:
            description.append([InlineKeyboardButton(reactions, callback_data="void")])
        effective_reply_markup = reply_markup if isinstance(reply_markup, InlineKeyboardMarkup) else InlineKeyboardMarkup([])
        effective_reply_markup.inline_keyboard = description + effective_reply_markup.inline_keyboard
        return effective_reply_markup


    def slave_message_file(self, msg: Message, tg_dest: TelegramChatID,
                           thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                           old_msg_id: OldMsgID = None,
                           target_msg_id: Optional[TelegramMessageID] = None,
                           target_quote_text: Optional[str] = None,
                           reply_markup: Optional[ReplyMarkup] = None,
                           silent: bool = False) -> telegram.Message:
        self.bot.send_chat_action(tg_dest, ChatAction.UPLOAD_DOCUMENT, message_thread_id=thread_id)

        if msg.filename is None and msg.path is not None:
            file_name = os.path.basename(msg.path)
        else:
            assert msg.filename is not None  # mypy compliance
            file_name = msg.filename

        # Telegram Bot API drops everything after `;` in filenames
        # Replace it with a space
        # Note: it also seems to strip off a lot of unicode punctuations
        file_name = file_name.replace(';', ' ')

        if msg.text:
            text = self.html_substitutions(msg)
        elif msg_template:
            placeholder_flag = self.flag("default_media_prompt")
            if placeholder_flag == "emoji":
                text = "📄"
            elif placeholder_flag == "text":
                text = self._("Sent a file.")
            else:
                text = ""
        else:
            text = ""
        text_prefix = self.format_text_message_template(msg_template, msg.author)

        try:
            file_too_large = self.check_file_size(msg.file)
            edit_media = msg.edit_media
            if file_too_large:
                if old_msg_id:
                    if msg.edit_media:
                        edit_media = False
                    self.bot.send_message(chat_id=old_msg_id[0], reply_to_message_id=old_msg_id[1], text=file_too_large)
                else:
                    message = self.bot.send_message(chat_id=tg_dest,
                                                    message_thread_id=thread_id, text=text,
                                                    parse_mode="HTML", reply_markup=reply_markup,
                                                    disable_notification=silent,
                                                    prefix=text_prefix, suffix=reactions,
                                                    **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
                    message.reply_text(file_too_large)
                    return message

            if old_msg_id:
                if edit_media:
                    assert msg.file is not None and msg.path is not None
                    file = self.process_file_obj(msg.file, msg.path)
                    return self.bot.edit_message_media(chat_id=old_msg_id[0], message_id=old_msg_id[1], media=InputMediaDocument(file, filename=msg.filename), reply_markup=reply_markup,
                                                            prefix=text_prefix, suffix=reactions, caption=text, parse_mode="HTML")
                return self.bot.edit_message_caption(chat_id=old_msg_id[0], message_id=old_msg_id[1], reply_markup=reply_markup,
                                                     prefix=text_prefix, suffix=reactions, caption=text, parse_mode="HTML")
            assert msg.file is not None and msg.path is not None
            self.logger.debug("[%s] Uploading file %s (%s) as %s", msg.uid,
                              msg.file.name, msg.mime, file_name)
            file = self.process_file_obj(msg.file, msg.path)
            return self.bot.send_document(tg_dest, file,
                                          prefix=text_prefix, suffix=reactions,
                                          caption=text, parse_mode="HTML", filename=file_name,
                                          message_thread_id=thread_id,
                                          reply_markup=reply_markup,
                                          disable_notification=silent,
                                          **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
        finally:
            if msg.file is not None:
                msg.file.close()

    def slave_message_voice(self, msg: Message, tg_dest: TelegramChatID,
                            thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                            old_msg_id: OldMsgID = None,
                            target_msg_id: Optional[TelegramMessageID] = None,
                            target_quote_text: Optional[str] = None,
                            reply_markup: Optional[ReplyMarkup] = None,
                            silent: bool = False) -> telegram.Message:
        self.bot.send_chat_action(tg_dest, ChatAction.RECORD_AUDIO, message_thread_id=thread_id)
        if msg.text:
            text = self.html_substitutions(msg)
        else:
            text = ""
        text_prefix = self.format_text_message_template(msg_template, msg.author)
        self.logger.debug("[%s] Message is a voice file.", msg.uid)
        try:
            file_too_large = self.check_file_size(msg.file)
            edit_media = msg.edit_media
            if file_too_large:
                if old_msg_id:
                    if msg.edit_media:
                        edit_media = False
                    self.bot.send_message(chat_id=old_msg_id[0], reply_to_message_id=old_msg_id[1], text=file_too_large)
                else:
                    message = self.bot.send_message(chat_id=tg_dest,
                                                    message_thread_id=thread_id, text=text,
                                                    parse_mode="HTML", reply_markup=reply_markup,
                                                    disable_notification=silent,
                                                    prefix=text_prefix, suffix=reactions,
                                                    **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
                    message.reply_text(file_too_large)
                    return message

            if old_msg_id:
                if edit_media:
                    original_message = self.channel.get_message_by_id(msg.chat, old_msg_id[1])
                    if original_message and original_message.type == MsgType.Voice:
                        # Cannot edit voice message content, send a new one instead
                        msg_template += " " + self._("[Edited]")
                        text_prefix = self.format_text_message_template(msg_template, msg.author)
                        if str(tg_dest) == old_msg_id[0]:
                            target_msg_id = target_msg_id or old_msg_id[1]
                        old_msg_id = None
                    else:
                        assert msg.file is not None
                        with tempfile.NamedTemporaryFile() as f:
                            pydub.AudioSegment.from_file(msg.file).export(f, format="ogg", codec="libopus",
                                                                        parameters=['-vbr', 'on'])
                            file = self.process_file_obj(f, f.name)
                            return self.bot.edit_message_media(chat_id=old_msg_id[0], message_id=old_msg_id[1], media=InputMediaAudio(file), reply_markup=reply_markup,
                                                                    prefix=text_prefix, suffix=reactions, caption=text, parse_mode="HTML")
                else:
                    return self.bot.edit_message_caption(chat_id=old_msg_id[0], message_id=old_msg_id[1],
                                                         reply_markup=reply_markup, prefix=text_prefix,
                                                         suffix=reactions, caption=text, parse_mode="HTML")

            assert msg.file is not None
            with tempfile.NamedTemporaryFile() as f:
                pydub.AudioSegment.from_file(msg.file).export(f, format="ogg", codec="libopus",
                                                              parameters=['-vbr', 'on'])
                file = self.process_file_obj(f, f.name)
                tg_msg = self.bot.send_voice(tg_dest, file, prefix=text_prefix, suffix=reactions,
                                             caption=text, parse_mode="HTML",
                                             reply_markup=reply_markup,
                                             message_thread_id=thread_id,
                                             disable_notification=silent,
                                             **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
            return tg_msg
        finally:
            if msg.file is not None:
                msg.file.close()

    def slave_message_location(self, msg: Message, tg_dest: TelegramChatID,
                               thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                               old_msg_id: OldMsgID = None,
                               target_msg_id: Optional[TelegramMessageID] = None,
                               target_quote_text: Optional[str] = None,
                               reply_markup: Optional[InlineKeyboardMarkup] = None,
                               silent: bool = False) -> telegram.Message:
        # Location messages cannot be edited in content by bots.
        # If an edit request comes, send a new message replying to the old one.
        self.bot.send_chat_action(tg_dest, ChatAction.FIND_LOCATION, message_thread_id=thread_id)
        assert (isinstance(msg.attributes, LocationAttribute))
        attributes: LocationAttribute = msg.attributes
        self.logger.info("[%s] Sending as a Telegram venue.\nlat: %s, long: %s\ntitle: %s\naddress: %s",
                         msg.uid,
                         attributes.latitude, attributes.longitude,
                         msg.text, msg_template)

        self.logger.debug("[%s] Location message received with old_msg_id %s, compare it with tg_dest %s", msg.uid, old_msg_id, tg_dest)
        if old_msg_id and old_msg_id[0] == str(tg_dest):
            # TRANSLATORS: Flag for messages edited on slave channels, but cannot be edited on Telegram.
            msg_template += " " + self._('[edited]')
            target_msg_id = target_msg_id or old_msg_id[1]
            self.logger.debug("[%s] updated target_msg_id %s", msg.uid, target_msg_id)

        location_reply_markup = self.build_chat_info_inline_keyboard(msg, msg_template, reactions, reply_markup)
        name = urllib.parse.quote(html.escape(msg.text))
        content = urllib.parse.quote(html.escape(msg.author.long_name))
        baidu = f'https://api.map.baidu.com/marker?location={attributes.latitude},{attributes.longitude}&title={name}&content={content}&output=html&coord_type=gcj02'
        # gaode require login on pc
        gaode = f'https://uri.amap.com/marker?position={attributes.longitude},{attributes.latitude}&name={name}&coordinate=gaode&callnative=1'
        tencent = f'https://apis.map.qq.com/uri/v1/marker?marker=coord:{attributes.latitude},{attributes.longitude};title:{content};addr:{name}'
        location_reply_markup.inline_keyboard = location_reply_markup.inline_keyboard + [
            [InlineKeyboardButton(self._('Baidu'), url=baidu), InlineKeyboardButton(self._('Gaode'), url=gaode), InlineKeyboardButton(self._('Tencent'), url=tencent)],
        ]

        # TODO: Use live location if possible? Lift live location messages to EFB Framework?
        return self.bot.send_location(tg_dest, latitude=attributes.latitude,
                                      longitude=attributes.longitude,
                                      message_thread_id=thread_id,
                                      reply_markup=location_reply_markup,
                                      disable_notification=silent,
                                      **self.build_reply_target_kwargs(target_msg_id, target_quote_text))

    def slave_message_video(self, msg: Message, tg_dest: TelegramChatID,
                            thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                            old_msg_id: OldMsgID = None,
                            target_msg_id: Optional[TelegramMessageID] = None,
                            target_quote_text: Optional[str] = None,
                            reply_markup: Optional[ReplyMarkup] = None,
                            silent: bool = False) -> telegram.Message:
        self.bot.send_chat_action(tg_dest, ChatAction.UPLOAD_VIDEO, message_thread_id=thread_id)
        if msg.text:
            text = self.html_substitutions(msg)
        elif msg_template:
            placeholder_flag = self.flag("default_media_prompt")
            if placeholder_flag == "emoji":
                text = "🎥"
            elif placeholder_flag == "text":
                text = self._("Sent a video.")
            else:
                text = ""
        else:
            text = ""
        text_prefix = self.format_text_message_template(msg_template, msg.author)
        try:
            file_too_large = self.check_file_size(msg.file)
            edit_media = msg.edit_media
            if file_too_large:
                if old_msg_id:
                    if msg.edit_media:
                        edit_media = False
                    self.bot.send_message(chat_id=old_msg_id[0], reply_to_message_id=old_msg_id[1], text=file_too_large)
                else:
                    message = self.bot.send_message(chat_id=tg_dest,
                                                    message_thread_id=thread_id, text=text,
                                                    parse_mode="HTML", reply_markup=reply_markup,
                                                    disable_notification=silent,
                                                    prefix=text_prefix, suffix=reactions,
                                                    **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
                    message.reply_text(file_too_large)
                    return message

            if old_msg_id:
                if edit_media:
                    assert msg.file is not None and msg.path is not None
                    file = self.process_file_obj(msg.file, msg.path)
                    return self.bot.edit_message_media(chat_id=old_msg_id[0], message_id=old_msg_id[1], media=InputMediaVideo(file, filename=msg.filename), reply_markup=reply_markup,
                                                         prefix=text_prefix, suffix=reactions, caption=text, parse_mode="HTML")
                return self.bot.edit_message_caption(chat_id=old_msg_id[0], message_id=old_msg_id[1], reply_markup=reply_markup,
                                                     prefix=text_prefix, suffix=reactions, caption=text, parse_mode="HTML")
            assert msg.file is not None and msg.path is not None
            file = self.process_file_obj(msg.file, msg.path)
            return self.bot.send_video(tg_dest, file, prefix=text_prefix, suffix=reactions,
                                       caption=text, parse_mode="HTML",
                                       message_thread_id=thread_id,
                                       reply_markup=reply_markup,
                                       disable_notification=silent,
                                       **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
        finally:
            if msg.file is not None:
                msg.file.close()

    def slave_message_unsupported(self, msg: Message, tg_dest: TelegramChatID,
                                  thread_id: Optional[TelegramTopicID], msg_template: str, reactions: str,
                                  old_msg_id: OldMsgID = None,
                                  target_msg_id: Optional[TelegramMessageID] = None,
                                  target_quote_text: Optional[str] = None,
                                  reply_markup: Optional[ReplyMarkup] = None,
                                  silent: bool = False) -> telegram.Message:
        self.logger.debug("[%s] Sending as an unsupported message.", msg.uid)
        # Note: send_chat_action for unsupported might need adjustment if PTB changes behavior
        self.bot.send_chat_action(tg_dest, ChatAction.TYPING, message_thread_id=thread_id)
        if msg.text:
            text = self.html_substitutions(msg)
        else:
            text = ""
        unsupported_prefix = self.format_text_message_template(msg_template + " " + self._("(unsupported)"), msg.author)
        unsupported_edited_prefix = self.format_text_message_template(
            msg_template + " " + self._("(unsupported) [Edited]"), msg.author
        )

        if not old_msg_id:
            tg_msg = self.bot.send_message(tg_dest,
                                           text=text, parse_mode="HTML",
                                           prefix=unsupported_prefix,
                                           suffix=reactions,
                                           message_thread_id=thread_id, reply_markup=reply_markup,
                                           disable_notification=silent,
                                           **self.build_reply_target_kwargs(target_msg_id, target_quote_text))
        else:
            # Cannot change reply_to_message_id or thread_id when editing a message
            tg_msg = self.bot.edit_message_text(chat_id=old_msg_id[0],
                                                message_id=old_msg_id[1],
                                                text=text, parse_mode="HTML",
                                                prefix=unsupported_edited_prefix,
                                                suffix=reactions,
                                                reply_markup=reply_markup)

        self.logger.debug("[%s] Processed and sent as text message", msg.uid)
        return tg_msg

    def slave_message_status(self, msg: Message, tg_dest: TelegramChatID,
                             thread_id: Optional[TelegramTopicID]):
        attributes = msg.attributes
        assert isinstance(attributes, StatusAttribute)
        if attributes.status_type is StatusAttribute.Types.TYPING:
            self.bot.send_chat_action(tg_dest, ChatAction.TYPING, message_thread_id=thread_id)
        elif attributes.status_type is StatusAttribute.Types.UPLOADING_VOICE:
            self.bot.send_chat_action(tg_dest, ChatAction.RECORD_AUDIO, message_thread_id=thread_id)
        elif attributes.status_type is StatusAttribute.Types.UPLOADING_IMAGE:
            self.bot.send_chat_action(tg_dest, ChatAction.UPLOAD_PHOTO, message_thread_id=thread_id)
        elif attributes.status_type is StatusAttribute.Types.UPLOADING_VIDEO:
            self.bot.send_chat_action(tg_dest, ChatAction.UPLOAD_VIDEO, message_thread_id=thread_id)
        elif attributes.status_type is StatusAttribute.Types.UPLOADING_FILE:
            self.bot.send_chat_action(tg_dest, ChatAction.UPLOAD_DOCUMENT, message_thread_id=thread_id)

    def send_status(self, status: Status):
        if isinstance(status, ChatUpdates):
            self.logger.debug("Received chat updates from channel %s", status.channel)
            for i in status.removed_chats:
                self.db.delete_slave_chat_info(status.channel.channel_id, i)
                self.chat_manager.delete_chat_object(status.channel.channel_id, i)
            for i in itertools.chain(status.new_chats, status.modified_chats):
                chat = status.channel.get_chat(i)
                self.chat_manager.update_chat_obj(chat, full_update=True)
        elif isinstance(status, MemberUpdates):
            self.logger.debug("Received member updates from channel %s about group %s",
                              status.channel, status.chat_id)
            for i in status.removed_members:
                self.db.delete_slave_chat_info(status.channel.channel_id, i, status.chat_id)
            self.chat_manager.delete_chat_members(status.channel.channel_id, status.chat_id, status.removed_members)
            chat = status.channel.get_chat(status.chat_id)
            self.chat_manager.update_chat_obj(chat, full_update=True)
        elif isinstance(status, MessageRemoval):
            self.logger.debug("Received message removal request from channel %s on message %s",
                              status.source_channel, status.message)
            old_msg = self.db.get_msg_log(
                slave_msg_id=status.message.uid,
                slave_origin_uid=utils.chat_id_to_str(chat=status.message.chat))
            if old_msg:
                old_msg_id: OldMsgID = utils.message_id_str_to_id(old_msg.master_msg_id)
                self.logger.debug("Found message to delete in Telegram: %s.%s",
                                  *old_msg_id)
                try:
                    if not self.channel.flag('prevent_message_removal'):
                        self.bot.delete_message(*old_msg_id)
                        return
                except TelegramError as e:
                    self.logger.warning("Failed to delete message %s.%s: %s. Sending notification instead.", *old_msg_id, e)
                    pass
                self.bot.send_message(chat_id=old_msg_id[0],
                                      text=self._("Message is removed in remote chat."),
                                      reply_to_message_id=old_msg_id[1],
                                      disable_notification=True) # Probably silent notification
            else:
                self.logger.info('Was supposed to delete a message, '
                                 'but it does not exist in database: %s', status)
        elif isinstance(status, MessageReactionsUpdate):
            self.update_reactions(status)
        else:
            self.logger.error('Received an unsupported type of status: %s', status)

    @staticmethod
    def build_reactions_footer(reactions: Reactions) -> str:
        """Generate a footer string for reactions in the format similar to [🙂×3, ❤️×1].
        Returns '' if no reaction is found.
        """
        result = "[" + ", ".join(f"{k}×{len(v)}" for k, v in reactions.items() if len(v)) + "]"
        if result == "[]":
            return ""
        return result

    def update_reactions(self, status: MessageReactionsUpdate):
        """Update reactions to a Telegram message."""
        old_msg_db = self.db.get_msg_log(slave_msg_id=status.msg_id,
                                         slave_origin_uid=utils.chat_id_to_str(chat=status.chat))
        if old_msg_db is None:
            self.logger.exception('Trying to update reactions of message, but message is not found in database. '
                                  'Message ID %s from %s, status: %s.', status.msg_id, status.chat, status.reactions)
            return

        old_msg: ETMMsg = old_msg_db.build_etm_msg(chat_manager=self.chat_manager)
        old_msg.reactions = status.reactions
        old_msg.edit = True # Mark as edit so dispatch knows it's an update
        old_msg.edit_media = False # Ensure media is not considered edited

        msg_template, _ = self.get_slave_msg_dest(old_msg)
        effective_msg = old_msg_db.master_msg_id_alt or old_msg_db.master_msg_id
        chat_id, msg_id = utils.message_id_str_to_id(effective_msg)

        # Go through the ordinary update process
        self.dispatch_message(msg=old_msg, msg_template=msg_template, old_msg_id=(chat_id, msg_id), tg_dest=chat_id)

    @staticmethod
    def normalize_reply_quote_text(text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        quote = text.strip()
        if not quote:
            return None
        return quote[:1024]

    def build_reply_target_kwargs(self, target_msg_id: Optional[TelegramMessageID],
                                  target_quote_text: Optional[str] = None) -> dict:
        if not target_msg_id:
            return {}
        quote = self.normalize_reply_quote_text(target_quote_text)
        if quote:
            return {
                "api_kwargs": {
                    "reply_parameters": {
                        "message_id": target_msg_id,
                        "quote": quote
                    }
                }
            }
        return {"reply_to_message_id": target_msg_id}

    def resolve_author_format(self) -> str:
        author_format = self.flag("author_format")
        if author_format in ("bold", "blockquote"):
            return author_format
        return "bold"

    @staticmethod
    def build_author_template_html(author: Chat, fallback_text: str) -> str:
        alias = getattr(author, "alias", None)
        name = getattr(author, "name", None)
        alias = alias if isinstance(alias, str) else ""
        name = name if isinstance(name, str) else ""
        if alias and name:
            needle = f"{alias} ({name})"
            if needle in fallback_text:
                before, _, after = fallback_text.partition(needle)
                return (
                    html.escape(before)
                    + f"{html.escape(alias)} (<tg-spoiler>{html.escape(name)}</tg-spoiler>)"
                    + html.escape(after)
                )
        return html.escape(fallback_text)

    def format_text_message_template(self, msg_template: str, author: Optional[Chat] = None) -> str:
        if not msg_template:
            return ""
        author_text = html.escape(msg_template)
        if author:
            author_text = self.build_author_template_html(author, msg_template)
        if self.resolve_author_format() == "blockquote":
            return f"<blockquote>{author_text}</blockquote>"
        return f"<b>{author_text}</b>"

    def generate_message_template(self, msg: Message, singly_linked: bool) -> str:
        msg_prefix = ""  # For group member name
        if isinstance(msg.chat, GroupChat):
            self.logger.debug("[%s] Message is from a group. Sender: %s", msg.uid, msg.author)
            msg_prefix = msg.author.long_name

        if singly_linked:
            if msg_prefix:  # if group message
                msg_template = f"{msg_prefix}:"
            else:
                if msg.chat != msg.author:
                    msg_template = f"{msg.author.long_name}:"
                else:
                    msg_template = ""
        elif isinstance(msg.chat, PrivateChat):
            emoji_prefix = msg.chat.channel_emoji + Emoji.USER
            name_prefix = msg.chat.long_name
            if msg.chat.other != msg.author:
                name_prefix += f", {msg.author.long_name}"
            msg_template = f"{emoji_prefix} {name_prefix}:"
        elif isinstance(msg.chat, GroupChat):
            emoji_prefix = msg.chat.channel_emoji + Emoji.GROUP
            name_prefix = msg.chat.long_name
            msg_template = f"{emoji_prefix} {msg_prefix} [{name_prefix}]:"
        elif isinstance(msg.chat, SystemChat):
            emoji_prefix = msg.chat.channel_emoji + Emoji.SYSTEM
            name_prefix = msg.chat.long_name
            if msg.chat.other != msg.author:
                name_prefix += f", {msg.author.long_name}"
            msg_template = f"{emoji_prefix} {name_prefix}:"
        else:
            msg_template = f"{Emoji.UNKNOWN} {msg.author.long_name} ({msg.chat.display_name}):"
        return msg_template

    def check_file_size(self, file: Optional[IO[bytes]]) -> Optional[str]:
        """
        Return an error message if the file is too large to upload,
        None otherwise.
        """
        if not file or getattr(file, "closed", True):
            return None
        file.seek(0, 2)
        file_size = file.tell()
        file.seek(0)
        if not self.channel.flag("local_tdlib_api") and file_size > telegram.constants.MAX_FILESIZE_UPLOAD:
            size_str = humanize.naturalsize(file_size)
            max_size_str = humanize.naturalsize(telegram.constants.MAX_FILESIZE_UPLOAD)
            return self._(
                "Attachment is too large ({size}). Maximum allowed by Telegram Bot API is {max_size}. (AT02)").format(
                size=size_str, max_size=max_size_str)
        return None

    def process_file_obj(self, file: IO[bytes], path: Union[str, Path]) -> Union[IO[bytes], str]:
        if self.channel.flag("local_tdlib_api"):
            return Path(path).absolute().as_uri()
        return file

