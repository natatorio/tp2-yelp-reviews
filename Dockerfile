FROM python:latest
RUN pip install pika
RUN pip install Flask
RUN pip install requests
RUN pip install psycopg2
COPY . .
CMD python3 main.py
