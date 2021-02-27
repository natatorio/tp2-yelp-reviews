FROM python:latest
RUN pip install pika requests
COPY . .
CMD python3 main.py
