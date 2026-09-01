producer-api:
	uv run fastapi dev src/producer/main.py


create-migration:
	uv run alembic init -t async migrations

initialize-db:
	uv run alembic revision --autogenerate -m "initial schema"
	uv run alembic upgrade head