FROM python:3.9 AS base

RUN apt-get update --allow-releaseinfo-change -y
RUN apt-get upgrade --fix-missing -y
RUN apt-get install -y --fix-missing --no-install-recommends git

RUN cd /tmp && git clone -b dev-k https://github.com/quanted/nta_app.git

FROM python:3.9

RUN apt-get update --allow-releaseinfo-change -y
RUN apt-get upgrade --fix-missing -y
RUN apt-get install -y --fix-missing --no-install-recommends \
    python3-pip software-properties-common build-essential \
    cmake git sqlite3 gfortran python-dev && \
    pip install -U pip

COPY uwsgi.ini /etc/uwsgi/
COPY . /src/nta_flask
RUN chmod 755 /src/nta_flask/start_flask.sh
WORKDIR /src/
EXPOSE 8080

COPY --from=base /tmp/nta_app/requirements.txt /src/nta_flask/requirements.txt
RUN pip install -r /src/nta_flask/requirements.txt
RUN pip install uwsgi
RUN python --version

ENV PYTHONPATH /src:/src/nta_flask/:$PYTHONPATH
ENV PATH /src:/src/nta_flask/:$PATH

CMD ["sh", "/src/nta_flask/start_flask.sh"]
