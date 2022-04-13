install:
	poetry install

check: install
	poetry run prospector

build:
	poetry export -f requirements.txt --output requirements.txt --without-hashes
	docker build -t nekonyuu/movies-view-injector .

start:
	poetry run faust -A movies_views_injector worker -l info --web-port 6667