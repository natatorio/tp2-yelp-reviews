#!/usr/bin/env bash
set -euo pipefail

  # client:
  #   build:
  #     context: .
  #   entrypoint: python3 -m client.main
  #   environment:
  #     - PYTHONUNBUFFERED=1
  #     - AMQP_URL=amqp://rabbitmq?connection_attempts=5&retry_delay=5
  #   networks:
  #     - reviews_network
  #   volumes:
  #     - ../data:/data:rw
  #   depends_on:
  #     - rabbitmq
docker build . -t reviews
docker run -it --network="tp3_reviews_network" -v "$(cd .. && pwd)/data:/data" reviews 

