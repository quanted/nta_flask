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
COPY . /src/flask_nta
RUN chmod 755 /src/flask_nta/start_flask.sh
WORKDIR /src/
EXPOSE 8080

COPY --from=base /tmp/nta_app/requirements.txt /src/flask_nta/requirements.txt
RUN pip install -r /src/flask_nta/requirements.txt
RUN pip install uwsgi
RUN python --version

ENV PYTHONPATH /src/flask_nta/:$PYTHONPATH
ENV PATH /src/flask_nta/:$PATH

CMD ["sh", "/src/flask_nta/start_flask.sh"]
