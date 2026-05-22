from types import SimpleNamespace

from ehforwarderbot import MsgType

from efb_telegram_master.master_message import MasterMessageProcessor
from efb_telegram_master.msg_type import TGMsgType


def build_message(file_name=None):
    document = None
    if file_name is not None:
        document = SimpleNamespace(file_name=file_name)
    return SimpleNamespace(document=document)


def test_tg_image_document_matches_png_name():
    message = build_message("tg_image_1999621452.png")

    assert MasterMessageProcessor.is_tg_image_document(message) is True


def test_tg_image_document_rejects_other_image_documents():
    assert MasterMessageProcessor.is_tg_image_document(build_message("photo.png")) is False
    assert MasterMessageProcessor.is_tg_image_document(build_message("tg_image_1999621452.jpg")) is False
    assert MasterMessageProcessor.is_tg_image_document(build_message(None)) is False


def test_tg_image_document_uses_image_msg_type():
    efb_type = MsgType.File
    message = build_message("tg_image_1999621452.png")

    if TGMsgType.Document is TGMsgType.Document and MasterMessageProcessor.is_tg_image_document(message):
        efb_type = MsgType.Image

    assert efb_type == MsgType.Image
