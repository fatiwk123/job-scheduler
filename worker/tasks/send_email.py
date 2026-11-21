"""Envoi d'email via SMTP. En local, Mailpit (docker compose) capture les messages :
interface web sur http://localhost:8025."""

import smtplib
from email.message import EmailMessage
from email.utils import make_msgid

from shared.payloads import SendEmailPayload
from worker.tasks import TaskContext

SENDER = "job-scheduler@example.com"


def handle(payload: SendEmailPayload, ctx: TaskContext) -> dict:
    msg = EmailMessage()
    msg["From"] = SENDER
    msg["To"] = payload.to
    msg["Subject"] = payload.subject
    # Message-ID derive de l'id de tache : un retry apres un envoi deja reussi
    # produit le meme Message-ID, que les serveurs/clients peuvent dedupliquer.
    msg["Message-ID"] = make_msgid(idstring=ctx.task_id, domain="job-scheduler.local")
    msg.set_content(payload.body)

    # Erreurs reseau / SMTP -> exception -> retry avec backoff.
    with smtplib.SMTP(ctx.settings.smtp_host, ctx.settings.smtp_port, timeout=10) as smtp:
        smtp.send_message(msg)
    return {"to": payload.to, "message_id": msg["Message-ID"]}
