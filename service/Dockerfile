FROM python:3.8-slim
VOLUME /golem/in /golem/out
COPY ftse_client.py /golem/run/ftse_client.py
COPY ftse_server.py /golem/run/ftse_server.py
COPY requirements.txt /
RUN pip install -r requirements.txt
RUN chmod +rwx /golem/run/*
