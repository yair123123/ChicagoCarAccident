from pymongo.errors import PyMongoError
from returns.result import Success, Failure

from config.connect import daily, weekly, monthly, areas, accidents


def indexing_collection():
    try:
        daily.create_index({"date": 1})
        weekly.create_index({"start": 1})
        monthly.create_index({"year": 1, "month": 1})
        areas.create_index({"area": 1})
        return Success("indexing success")
    except PyMongoError as e:
        return Failure(str(e))


def get_accident_by_area(id_area: id):
    try:

        return Success(list(areas.aggregate([
            {'$match': {"area": id_area}},
            {
                '$lookup': {
                    'from': 'accidents',
                    'localField': 'accidents',
                    'foreignField': '_id',
                    'as': 'accidents'
                }
            }
        ])))
    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_week(id_area: id, date_start: str):
    date_start = date_start[0]
    try:
        accidents_by_area = areas.find({"area_id": id_area})["accidents"]
        monthly_accident = monthly.find({"start": date_start})["accidents"]
        accidents_by_area_monthly = list(set(accidents_by_area) & set(monthly_accident))
        results = accidents.find({'_id': {'$in': accidents_by_area_monthly}})
        return Success(results)
    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_day(id_area: id, date: str):
    date = date[0]
    try:
        accidents_by_area = areas.find({"area_id": id_area})["accidents"]
        daily_accident = daily.find({"date": date})["accidents"]
        accidents_by_area_daily = list(set(accidents_by_area) & set(daily_accident))
        results = accidents.find({'_id': {'$in': accidents_by_area_daily}})
        return Success(results)
    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_month(id_area: int, date: tuple):
    year = date[0]
    month = date[1]
    breakpoint()
    try:
        area_doc = areas.find_one({"area": id_area})
        if area_doc is None:
            return Failure(f"No accidents found for area {id_area}")
        accidents_by_area = area_doc["accidents"]
        weekly_accident_doc = weekly.find_one({"year": year, "month": month})
        if weekly_accident_doc is None or "accidents" not in weekly_accident_doc:
            return Failure(f"No weekly accidents found for {month}/{year}")
        weekly_accident = weekly_accident_doc["accidents"]
        accidents_by_area_weekly = list(set(accidents_by_area) & set(weekly_accident))
        results_cursor = accidents.find({'_id': {'$in': accidents_by_area_weekly}})
        results = list(results_cursor)

        return Success(results)

    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_cause_and_area(area_id: int):
    try:
        accidents_by_area = list(areas.aggregate([
            {"$match": {"area_id": area_id}},
            {"$lookup": {
                "from": "accidents",
                "localField": "accidents",
                "foreignField": "_id",
                "as": "accidents"
            }},
            {"$unwind": "$accidents"},
            {"$group": {
                "_id": "$accidents.PRIM_CONTRIBUTORY_CAUSE",
                "total_accidents": {"$sum": 1},
                "accidents": {"$push": "$accidents"}
            }}
        ]))
        return Success(accidents_by_area)
    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_injured_and_area(area_id: int):
    try:
        accidents_by_area = list(areas.aggregate([
            {"$match": {"area_id": area_id}},
            {"$lookup": {
                "from": "accidents",
                "localField": "accidents",
                "foreignField": "_id",
                "as": "accidents"
            }},
            {"$unwind": "$accidents"},
            {"$group": {
                "_id": "$accidents.injured",
                "total_accidents": {"$sum": 1},
                "accidents": {"$push": "$accidents"}
            }}
        ]))
        return Success(accidents_by_area)
    except PyMongoError as e:
        return Failure(str(e))
