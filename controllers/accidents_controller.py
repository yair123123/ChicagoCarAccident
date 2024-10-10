from flask import blueprints, request

from repository.accidents_repository import *
from repository.csv_repository import *
from service.date_period_service import get_accidents_by_period
from utils.json_utils import parse_json

accident_blueprint = blueprints.Blueprint("db", __name__)


@accident_blueprint.route("/init", methods=["GET"])
def init_db():
    return (init_chicago_accidents()
            .map(lambda u: (parse_json(u), 200))
            .value_or((parse_json({}), 404)))


@accident_blueprint.route("/indexing", methods=["GET"])
def indexing():
    return (indexing_collection()
            .map(lambda u: (parse_json(u), 200))
            .value_or((parse_json({}), 404)))


@accident_blueprint.route("/area/<int:id_area>", methods=["GET"])
def accident_by_area(id_area: int):
    response = get_accident_by_area(id_area)
    return (response
            .map(lambda u: (parse_json(u), 200))
            .value_or((parse_json({}), 404))
            )


@accident_blueprint.route("/period", methods=["GET"])
def accident_by_period():
    area_id = request.args.get('area_id', type=int)
    date = tuple(request.args.get('date', type=str).split("/"))
    period = request.args.get('period', type=str)
    response = get_accidents_by_period(area_id, period, date)
    return (response
            .map(lambda u: (parse_json(u), 200))
            .value_or((parse_json({}), 404))
            )


@accident_blueprint.route("/cause/<int:id_area>", methods=["GET"])
def accident_by_cause_area(id_area: int):
    response = get_accidents_by_cause_and_area(id_area)
    return (response
            .map(lambda u: (parse_json(u), 200))
            .value_or((parse_json({}), 404))
            )


@accident_blueprint.route("/injured/<int:area_id>", methods=["GET"])
def accident_by_injured_area(area_id: int):
    response = get_accidents_by_injured_and_area(area_id)
    return (response
            .map(lambda u: (parse_json(u), 200))
            .value_or((parse_json({}), 404))
            )
