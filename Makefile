SHELL := /bin/bash

ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

build:
	docker build -t pyspark-nasa-logs-lab .

deploy_container:
	docker run -it --rm -v $(ROOT_DIR)resources:/pyspark-nasa-logs-lab/resources -v $(ROOT_DIR)scripts:/pyspark-nasa-logs-lab/scripts pyspark-nasa-logs-lab

deploy_jupyter_notebook:
	docker run -it --rm -v $(ROOT_DIR):/home/jovyan/work --user root -p 8888:8888 jupyter/all-spark-notebook

.PHONY: install test

default: test

install:
		pipenv install --dev --skip-lock

test:
		PYTHONPATH=./src pytest -vv
