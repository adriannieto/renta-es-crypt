.PHONY: help test test-unit test-compile docker-build

IMAGE_NAME ?= renta-es-crypt
IMAGE_TAG ?= latest

help:
	@printf "Available targets:\n"
	@printf "  make test           Run the full local verification suite.\n"
	@printf "  make test-unit      Run the Python unit tests.\n"
	@printf "  make test-compile   Compile-check src and tests.\n"
	@printf "  make docker-build   Build the Docker image.\n"
	@printf "\n"
	@printf "Docker image override example:\n"
	@printf "  make docker-build IMAGE_NAME=renta-es-crypt IMAGE_TAG=dev\n"

test: test-unit test-compile

test-unit:
	python3 -m unittest discover -s tests

test-compile:
	python3 -m compileall src tests

docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
