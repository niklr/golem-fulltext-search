FROM python:3.8-slim
VOLUME /golem/in /golem/out
COPY ftse.py /golem/run/ftse.py
RUN pip install whoosh
RUN chmod +x /golem/run/*
