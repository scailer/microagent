clean:
	rm -fr dist/ *.eggs .eggs build/ .coverage htmlcov/ .mypy_cache/ .pytest_cache/ *.log *.egg-info
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*.pyc' | xargs rm -rf

test:
	python3 setup.py test

run_aioredis_example: 
	python3 examples/aioredis_server.py

run_pulsar_example:
	python3 examples/pulsar_server.py --redis-server=redis://localhost:6379/7 --signal-bus=redis://localhost:6379/7 --queue-broker=amqp://user:31415@localhost:5672/prod --log-level=debug
	#python3 examples/pulsar_server.py --redis-server=redis://localhost:6379/7 --signal-bus=redis://localhost:6379/7 --queue-broker=redis://localhost:6379/7
