from __future__ import annotations

from collections.abc import Callable
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from functools import lru_cache
from pathlib import Path
from typing import TypeVar

from django.conf import settings
from django.contrib.staticfiles import finders
from django.core.mail import EmailMessage
from django.template import loader

MIMEType = TypeVar("MIMEType", bound=MIMEBase)


@lru_cache(maxsize=10)
def create_attachment(
    path: str,
    mime_type: Callable[[bytes], MIMEType] = MIMEImage,  # type: ignore[assignment] # https://github.com/python/mypy/issues/3737
) -> MIMEType:
    real_path = finders.find(path)
    if not real_path:
        raise FileNotFoundError(f"File {path} not found")
    content = Path(real_path).read_bytes()
    attachment = mime_type(content)

    file_name = path.rsplit("/", maxsplit=1)[-1]
    attachment.add_header("Content-ID", file_name)
    return attachment


def send_mail(
    template_name: str,
    subject: str,
    to: list[str],
    from_: str = f"<{settings.DEFAULT_FROM_EMAIL}>",
    context: dict | None = None,
    attachments: list[str] | None = None,
    cc: list[str] | None = None,
):
    context = context or {}
    attachments = attachments or []

    html = loader.render_to_string(template_name, context)

    message = EmailMessage(
        subject=subject,
        body=html,
        from_email=from_,
        to=to,
        cc=cc,
        attachments=[create_attachment(file) for file in attachments],
    )
    message.content_subtype = "html"
    message.mixed_subtype = "related"
    message.send()
