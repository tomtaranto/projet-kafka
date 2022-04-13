from faust import Record
from enum import Enum


class ViewCategory(str, Enum):
    StartOnly = "start_only"
    Half = "half"
    Full = "full"


class Movie(Record):
    _id: int
    title: str
    adult: bool


class MovieView(Record):
    _id: int
    title: str
    adult: bool
    view_category: ViewCategory


class MovieLike(Record):
    _id: int
    score: float
