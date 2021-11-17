from app.core.notifications import NotificationHandler
from app.producer import publish_to_kafka
from loguru import logger


class NotificationService(NotificationHandler):

    def __init__(self, notification_channels: list):
        self.notification_channels = notification_channels
        self.email_info = None
        self.sms_info = None

    def send(self):
        if "email" in self.notification_channels and self.email_info:
            self.send_mail()
        elif "sms" in self.notification_channels and self.sms_info:
            self.send_sms()
        else:
            logger.error(
                "Notification Service Error....Failed to Publish to Kakfa"
            )

    def send_mail(self):
        publish_to_kafka(topic="account_verification", value=self.email_info)
        self.email_info = None

    def send_sms(self):
        publish_to_kafka(topic="account_verification", value=self.sms_info)
        self.sms_info = None
