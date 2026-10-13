.PHONY: test test-docker up down smoke

test:
	pytest -v

test-docker:
	docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm test

up:
	docker compose up --build -d

down:
	docker compose down -v

smoke:
	bash scripts/smoke_test.sh
