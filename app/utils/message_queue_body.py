from app.models import CustomerModel
import json
from app.schema import ReadCustomerSchema
from flask import url_for

customer_object_serializer = ReadCustomerSchema()


def kafka_mail_message(customer: CustomerModel):
    message = json.loads(customer_object_serializer.dumps(customer))
    message["verification_link"] = url_for(
        "customer.account_verification", token=customer.get_customer_token(),
        _external=True
    )
    message["service_channel"] = "email"
    return message


def kafka_sms_message(customer: dict, status: str):
    message = {
        "account": customer.get("username"),
        "activity": "login",
        "status": status,
        "service_channel": "sms"
    }
    return message
