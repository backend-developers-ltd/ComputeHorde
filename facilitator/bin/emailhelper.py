#!/usr/bin/env python3

# Copyright (c) 2018, Reef Technologies, BSD 3-Clause License

import argparse
import os
import smtplib
import sys
from collections import namedtuple
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import urlsplit


class GmailSender(namedtuple("SmtpAuthData", "server port user password")):
    def send(self, addr_from, addr_to, subject, message, files=tuple()):
        msg = MIMEMultipart("alternative")
        msg["To"] = addr_to
        msg["From"] = addr_from
        msg["Subject"] = subject

        text = "view the html version."
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(message, "html"))

        for file in files:
            part = MIMEBase("application", "octet-stream")
            with open(file, "rb") as stream:
                part.set_payload(stream.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{os.path.basename(file)}"',
            )
            msg.attach(part)

        s = smtplib.SMTP(self.server, self.port)
        s.ehlo()
        s.starttls()
        if self.password:
            s.login(self.user, self.password)
        s.sendmail(addr_from, addr_to, msg.as_string())
        s.quit()


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-t",
        "--to",
        required=True,
        action="store",
        dest="to_email",
        help="Destination address",
    )

    parser.add_argument(
        "-f",
        "--files",
        action="store",
        nargs="*",
        dest="files",
        help="Files to be send as attachments",
    )

    parser.add_argument(
        "-s",
        "--subject",
        action="store",
        dest="subject",
        help="Subject of Email",
    )

    result = parser.parse_args()
    return result


if __name__ == "__main__":
    parser_result = parse_arguments()
    email_creds = os.environ.get("EMAIL_CREDS")
    if not email_creds:
        sys.stderr.write("no EMAIL_CREDS environment variable!\nexport EMAIL_CREDS=user:password@server:port")
        sys.exit(2)

    try:
        email_creds = urlsplit(f"//{email_creds}")
        if not all([email_creds.username, email_creds.hostname, email_creds.port]):
            raise ValueError
    except ValueError:
        sys.stderr.write(
            "EMAIL_CREDS environment variable has wrong format!\nexport EMAIL_CREDS=user:password@server:port"
        )
        sys.exit(2)

    addr_to = parser_result.to_email
    files = parser_result.files or []
    if "@" in email_creds.username:
        addr_from = email_creds.username
    else:
        addr_from = f"{email_creds.username}@{email_creds.hostname}"

    print("Enter/Paste the message for email. Ctrl-%s to save it." % (os.name == "nt" and "Z" or "D"))
    message_lines = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        message_lines.append(line)

    subject = parser_result.subject
    message = "\n".join(message_lines)

    sender = GmailSender(email_creds.hostname, email_creds.port, email_creds.username, email_creds.password)
    print("Sending email...")
    sender.send(addr_from, addr_to, subject, message, files=files)
