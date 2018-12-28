run_aioredis_example: 
	python3 examples/aioredis_server.py

run_pulsar_example:
	python3 examples/pulsar_server.py --redis-server=redis://localhost:6379/7
