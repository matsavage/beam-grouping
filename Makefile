
run-test:
	python3 -m pytest --cov-report term-missing --cov beam_grouping

run-pipeline:
	python3 -m beam_grouping.beam_pipeline

run-publisher:
	python3 util/pubsub_publisher.py 