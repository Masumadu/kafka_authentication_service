# local imports
from app.core.service_result import handle_result
from app.schema import ReadCustomerSchema, CreateCustomerSchema
from app.repositories import CustomerRepository
from app.services import RedisService
from app.controllers import CustomerController

# third party imports
import pinject
from flask import Blueprint, request

customer = Blueprint("customer", __name__)


obj_graph_customer = pinject.new_object_graph(
    modules=None,
    classes=[CustomerController, CustomerRepository, RedisService]
)
customer_controller = obj_graph_customer.provide(CustomerController)


@customer.route("/", methods=["GET"])
def index():
    customers_data = customer_controller.index()
    return handle_result(customers_data, schema=ReadCustomerSchema, many=True)


@customer.route("/", methods=["POST"])
def create_customer():
    customer_data = customer_controller.create(request.json)
    return handle_result(customer_data, schema=CreateCustomerSchema)


@customer.route("/verify_acct/<token>", methods=["GET"])
def account_verification(token):
    return customer_controller.verify_account(token)


@customer.route("/signin", methods=["POST"])
def sign_in():
    return customer_controller.sign_in(request.json)
