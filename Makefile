.PHONY: install test run dry-run

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v

dry-run:
	python -m src.pipeline --dry-run

run:
	python -m src.pipeline --periodo 2000-2022
