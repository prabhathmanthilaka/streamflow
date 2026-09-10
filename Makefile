up:
	docker compose up -d

down:
	docker compose down

setup:
	python -m src.setup

producer:
	python -m src.producer --count 10 --interval 0.5

consumer:
	python -m src.consumer

dlq:
	python -m src.dlq_consumer

test:
	pytest -q
