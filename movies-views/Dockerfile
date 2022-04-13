FROM python:3.7

RUN mkdir -p /srv/app

WORKDIR /srv/app
ADD files /srv/app/files
ADD movies_views_injector /srv/app/movies_views_injector
ADD requirements.txt /srv/app/
RUN pip install -r requirements.txt

CMD ["faust", "-A", "movies_views_injector", "worker", "-l", "info"]