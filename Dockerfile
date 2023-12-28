# syntax=docker/dockerfile:1

FROM python:3.12.1-slim

WORKDIR /app
COPY . .

RUN apt-get update && \
    apt-get install default-jre -y && \
    pip install -r requirements.txt

CMD python main.py
