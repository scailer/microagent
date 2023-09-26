clean:
	rm -fr dist/ *.eggs .eggs build/ .coverage htmlcov/ .mypy_cache/ .pytest_cache/ *.log *.egg-info
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*.pyc' | xargs rm -rf

test:
	ruff microagent
	mypy microagent
	pytest tests

release: clean
	python setup.py sdist bdist_wheel
	python3 -m twine upload dist/*

run_redis_example: 
	python3 examples/aioredis_server.py
