clean:
	rm -fr dist/ *.eggs .eggs build/ .coverage htmlcov/ .mypy_cache/ .pytest_cache/ *.log *.egg-info
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*.pyc' | xargs rm -rf

test:
	python -m ruff microagent tests
	python -m mypy microagent
	python -m pytest tests

release: clean
	python3 -m pip install --upgrade build twine
	python3 -m build
	python3 -m twine upload dist/*

run_redis_example: 
	python3 examples/redis_server.py
