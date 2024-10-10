import pytest
from pymongo.collection import Collection
from returns.result import Success

from repository.accidents_repository import *
from service.date_period_service import get_accidents_by_period


@pytest.fixture(scope="function")
def collections(init_test_data):
    return init_test_data['accidents']


def test_indexing(collections: Collection):
    res = indexing_collection()
    assert isinstance(res, Success)


def test_get_by_area(collections: Collection):
    res = get_accident_by_area(225)
    assert isinstance(res, Success)
    assert len(res.unwrap()) > 0


def test_get_by_period(collections: Collection):
    res = get_accidents_by_period(225,"month",("3","2020"))
    assert isinstance(res, Success)
    assert res.unwrap() > 0

def test_get_by_cause_area(collections: Collection):
    res = get_accidents_by_cause_and_area(225)
    assert isinstance(res, Success)
    assert res.unwrap() > 0

def test_get_accidents_by_injured_area(collections: Collection):
    res = get_accidents_by_injured_and_area(225)
    assert isinstance(res, Success)
    assert res.unwrap() > 0