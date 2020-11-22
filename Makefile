SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

build:

.PHONY: build

rabbit-up:
	sudo docker-compose up -d --build rabbitmq
.PHONY: rabbit-up

rabbit-restart:
	sudo docker-compose restart rabbitmq
.PHONY: rabbit-restart

docker-compose-up:
	sudo docker-compose up --build client router users stars5 business funny histogram
.PHONY: docker-compose-up

docker-compose-down:
	sudo docker-compose -f docker-compose-dev.yml stop -t 1
	sudo docker-compose -f docker-compose-dev.yml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yml logs -f
.PHONY: docker-compose-logs
