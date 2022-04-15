import gzip
import json
import random

from .models import Movie, MovieView, ViewCategory, MovieLike


def _load_movies():
    with gzip.open("files/movie_ids_03_01_2020.json.gz", "rb") as movies:
        for row in movies.readlines():
            movie = json.loads(row.decode("utf-8").rstrip())
            yield Movie(_id=movie["id"], title=movie["original_title"], adult=movie["adult"])


movies = [m for m in _load_movies()][:200]
available_view_cats = [v for v in ViewCategory]


def generate_view():
    movie = movies[random.randint(0, len(movies) - 1)]
    return MovieView(
        _id=movie._id,
        title=movie.title,
        adult=movie.adult,
        view_category=available_view_cats[
            random.randint(0, len(available_view_cats) - 1)
        ],
    )


def generate_like():
    movie = movies[random.randint(0, len(movies) - 1)]
    return MovieLike(
        _id=movie._id,
        score=round(random.random() * 5, 2),
    )
