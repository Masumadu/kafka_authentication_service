import jwt
import pytest
import unittest
from flask import url_for
from config import Config
from tests import BaseTestCase
from unittest.mock import patch
from app.models import CustomerModel
from app.core.notifications import Notifier
from app.services.notification_service import NotificationService

notification_service = NotificationService(["email", "sms"])

NEW_DATA = {
    "name": "new_customer",
    "username": "new_customer_username",
    "email": "new_customer_email",
    "password": "new_customer_password"
}
UPDATE_INFO = {
    "name": "update_customer",
    "username": "update_customer_username",
    "email": "update_customer_email",
    "password": "update_customer_password"
}


class TestCustomerViews(BaseTestCase):
    @pytest.mark.customer
    def test_create_customer(self):
        with patch("app.services.notification_service.publish_to_kafka") as publish_to_kafka:
            response = self.client.post(url_for("customer.create_customer"),
                                        json=NEW_DATA)
        self.assertEqual(CustomerModel.query.count(), 2)
        self.assertEqual(response.status_code, 201)
        self.assertIsInstance(response.json, dict)
        self.assertEqual(response.json["id"], 2)
        self.assertEqual(response.json["name"], NEW_DATA["name"])
        self.assertEqual(response.json["email"], NEW_DATA["email"])
        self.assertEqual(response.json["username"], NEW_DATA["username"])
        self.assertTrue(publish_to_kafka.called)

    @pytest.mark.customer
    def test_customer_signin_invalid_details(self):
        customer_info = {
            "username": "username",
            "password": "password"
        }
        with patch("app.services.notification_service.publish_to_kafka") as publish_to_kafka:
            response = self.client.post(
                url_for("customer.sign_in"), json=customer_info
            )
        self.assert200(response)
        self.assertIsInstance(response.json, dict)
        self.assertEqual(self.shared_responses.signin_invalid_details(),
                         response.json)
        self.assertTrue(publish_to_kafka.called)

    @pytest.mark.customer
    def test_customer_signin_unverified(self):
        customer_info = {
            "username": self.customer.get("username"),
            "password": self.customer.get("password")
        }
        with patch(
                "app.services.notification_service.publish_to_kafka") as publish_to_kafka:
            response = self.client.post(
                url_for("customer.sign_in"), json=customer_info
            )
        self.assert200(response)
        self.assertIsInstance(response.json, dict)
        self.assertEqual(
            self.shared_responses.signin_unverified_account(), response.json
        )
        self.assertTrue(publish_to_kafka.called)

    @pytest.mark.customer
    def test_customer_account_verification(self):
        unverified_customer = CustomerModel.query.get(1)
        self.assertFalse(unverified_customer.verification_status)
        token = jwt.encode({'acct_id': unverified_customer.id},
                           Config.SECRET_KEY, algorithm=Config.JWT_ALGORITHM)
        invalid_token = self.client.get(
            url_for("customer.account_verification",
                    token=token + "odsfosldfj")
        )
        self.assert200(invalid_token)
        self.assertEqual(
            invalid_token.json,
            self.shared_responses.account_verification_invalid_token()
        )
        valid_token = self.client.get(
            url_for("customer.account_verification", token=token)
        )
        self.assert200(valid_token)
        self.assertEqual(
            valid_token.json,
            self.shared_responses.account_verification_valid_token()
        )
        verified_customer = CustomerModel.query.get(1)
        self.assertTrue(verified_customer.verification_status)

    @pytest.mark.customer
    def test_customer_signin(self):
        customer = CustomerModel.query.get(1)
        token = jwt.encode({'acct_id': customer.id},
                           Config.SECRET_KEY, algorithm=Config.JWT_ALGORITHM)
        response = self.client.get(
            url_for("customer.account_verification", token=token)
        )
        self.assert200(response)
        self.assertEqual(
            response.json,
            self.shared_responses.account_verification_valid_token()
        )
        customer_info = {
            "username": self.customer.get("username"),
            "password": self.customer.get("password")
        }
        with patch("app.services.notification_service.publish_to_kafka") as publish_to_kafka:
            response = self.client.post(
                url_for("customer.sign_in"), json=customer_info
            )
        self.assert200(response)
        self.assertIsInstance(response.json, dict)
        self.assertEqual(self.shared_responses.signin_valid_details().keys(),
                         response.json.keys())
        self.assertTrue(publish_to_kafka.called)

    @pytest.mark.customer
    def test_notification_event_signal(self):
        with patch.object(Notifier, "notify") as mock_notifier:
            self.client.post(url_for("customer.create_customer"),
                             json=NEW_DATA)
        self.assertTrue(mock_notifier.called)
        self.assertEqual(mock_notifier.call_count, 1)

    @pytest.mark.customer
    def test_notification_service(self):
        with patch.object(NotificationService, "send") as mock_send:
            self.client.post(url_for("customer.create_customer"),
                             json=NEW_DATA)
        self.assertTrue(mock_send.called)
        self.assertEqual(mock_send.call_count, 1)


if __name__ == "__main__":
    unittest.main()
