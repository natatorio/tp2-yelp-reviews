FROM python:latest
RUN pip install pika
RUN pip install Flask
RUN pip install requests
RUN apt-get update
RUN apt install docker.io libev-dev -y 
RUN docker --version
RUN pip install docker
RUN pip install bjoern 
COPY . .
ENV PYTHONUNBUFFERED=1
ENV AMQP_URL=amqp://rabbitmq?connection_attempts=5&retry_delay=5&heartbeat=30
CMD python3 -m client.main
