from app.controllers import CustomerController
from app.repositories import CustomerRepository
from tests import BaseTestCase
import pytest
from app.models import CustomerModel
import unittest
from unittest.mock import patch
from app.core.service_result import ServiceResult

NEW_DATA = {
    "name": "new_name",
    "username": "new_username",
    "email": "new_email",
    "password": "new_password"
}


class TestCustomerController(BaseTestCase):
    @pytest.mark.customer
    def test_index(self):
        get_all_data = CustomerController(CustomerRepository(self.redis)).index()
        self.assertEqual(CustomerModel.query.count(), 1)
        self.assertIsInstance(get_all_data, ServiceResult)
        self.assertTrue(get_all_data.success)
        self.assert200(get_all_data.data)
        self.assertEqual(get_all_data.exception_case, None)

    @pytest.mark.customer
    def test_create(self):
        with patch("app.services.notification_service.publish_to_kafka") as publish_to_kafka:
            create_new_data = CustomerController(
                CustomerRepository(self.redis)
            ).create(NEW_DATA)
        self.assertEqual(CustomerModel.query.count(), 2)
        self.assertIsInstance(create_new_data, ServiceResult)
        self.assertTrue(create_new_data.success)
        self.assertEqual(create_new_data.data.status_code, 201)
        self.assertEqual(create_new_data.exception_case, None)
        self.assertTrue(publish_to_kafka.called)


if __name__ == "__main__":
    unittest.main()
