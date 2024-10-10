from flask import blueprints, request

from repository.car_repository import *
from utils.json_utils import parse_json

driver_blueprints = blueprints.Blueprint("driver", __name__)


@driver_blueprints.route("/", methods=["GET"])
def get_all_drivers():
    return (find_all_cars()
            .map(lambda u: (parse_json(u), 200))
            .value_or((parse_json({}), 404)))


@driver_blueprints.route("/<string:car_id>", methods=["GET"])
def get_car_by_id(car_id: str):
    return (find_car_by_id(car_id)
            .map(lambda u: (parse_json(u), 200))
            .value_or((parse_json({}), 404)))


@driver_blueprints.route("/create", methods=["POST"])
def create_car():
    car = request.json
    response = insert_car(car)
    return (response.map(lambda u: parse_json(u), 200)
            .value_or(parse_json({}), 404)
            )


@driver_blueprints.route("/update/<string:id>", methods=["PUT"])
def update_car(id: int):
    car = request.json
    response = update_car(car)
    return (response
            .map(lambda u: parse_json(u), 200)
            .value_or(parse_json({}), 404)
            )


@driver_blueprints.route("/delete/<string:id>", methods=["DELETE"])
def delete_car(id: int):
    response = delete_car(id)
    return (response
            .map(lambda u: parse_json(u), 200)
            .value_or(parse_json({}), 404)
            )
