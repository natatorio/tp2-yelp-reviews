FROM python:latest
RUN pip install pika requests flask 
COPY . .
CMD python3 main.py
