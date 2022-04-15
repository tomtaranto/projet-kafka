import os
import ssl

import faust

from .models import MovieLike, MovieView
from .source import generate_like, generate_view

SSL_CONTEXT = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")

app = faust.App("movies-views-injector", broker=f"kafka://{BROKER_HOST}")

views_topic = app.topic("views", key_type=str, value_type=MovieView)
likes_topic = app.topic("likes", key_type=str, value_type=MovieLike)


@app.timer(interval=0.03)
async def view_sender(app):
    view = generate_view()
    await views_topic.send(key=str(view._id), value=view)


@app.timer(interval=0.03)
async def like_sender(app):
    like = generate_like()
    await likes_topic.send(key=str(like._id), value=like)
