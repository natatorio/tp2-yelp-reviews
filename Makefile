SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

build:

.PHONY: build

clean-disk:
	sudo docker container stop $$(sudo docker ps -a -q)
	sudo docker rm $$(sudo docker ps -a -q)
	sudo docker volume rm $$(sudo docker volume ls -q)
.PHONY: clean-disk

rabbit-up:
	sudo docker-compose up -d --build rabbitmq
.PHONY: rabbit-up

rabbit-restart:
	sudo docker-compose restart rabbitmq
.PHONY: rabbit-restart

docker-compose-up:
	time sudo docker-compose up -d --build --scale watchdog=3 --scale router=2 --scale stars5_mapper=2 --scale histogram_mapper=2 --scale funny_mapper=2 --scale comment_mapper=2 --scale kevasto=3
.PHONY: docker-compose-up

docker-compose-down:
	sudo docker-compose -f docker-compose.yml stop -t 1
	sudo docker-compose -f docker-compose.yml down
.PHONY: docker-compose-down

docker-compose-logs:
	sudo docker-compose -f docker-compose.yml logs -f
.PHONY: docker-compose-logs
