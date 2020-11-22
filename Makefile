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
	time sudo docker-compose up --build client router users stars5 business funny histogram comment
.PHONY: docker-compose-up

docker-compose-down:
	sudo docker-compose -f docker-compose-dev.yml stop -t 1
	sudo docker-compose -f docker-compose-dev.yml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yml logs -f
.PHONY: docker-compose-logs