FROM python:latest
RUN pip install pika
RUN pip install Flask
RUN pip install requests
RUN apt-get update
RUN apt install docker.io -y
RUN docker --version
RUN pip install docker
COPY . .
CMD python3 main.py
