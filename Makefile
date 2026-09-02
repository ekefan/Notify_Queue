.PHONY: requirements migrate migration postgres-up producer-api v2-up v2-up-port-8001 v2-down v2-logs test

requirements:
	bash install_requirements.sh

producer-api:
	uv run fastapi dev src/producer/main.py

migrate:
	uv run alembic upgrade head

migration:
	@test -n "$(name)" || (echo "Usage: make migration name='describe change'" && exit 2)
	uv run alembic revision --autogenerate -m "$(name)"

postgres-up:
	docker compose up postgres -d

v2-up:
	docker compose --profile v2 up --build -d

v2-up-port-8001:
	API_PORT=8001 docker compose --profile v2 up --build

v2-down:
	docker compose --profile v2 down

v2-logs:
	docker compose --profile v2 logs -f api publisher worker-v2 rabbitmq

test:
	uv run pytest -q
