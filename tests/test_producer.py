from tests import BaseTestCase
import pytest
import unittest
from app.services.notification_service import NotificationService
from unittest.mock import patch
# from app.producer import PikaClient

notification_service = NotificationService(["email"])

email_parameters = {
    "from_email": "michaelasumadu1@outlook.com",
    "to_emails": "michaelasumadu1@gmail.com",
    "subject": "Invoice",
    "html_content": "<b> name</b>"
}


# class TestProducer(BaseTestCase):
#     @pytest.mark.producer
#     def test_pika_publish(self):
#         with patch.object(PikaClient, "publish_message") as mocked_publish:
#             notification_service.email_info = email_parameters
#             notification_service.send()
#         self.assertTrue(mocked_publish.called)
#         self.assertEqual(mocked_publish.call_count, 1)


if __name__ == "__main__":
    unittest.main()
