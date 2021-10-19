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
COPY . /src/
RUN chmod 755 /src/start_flask.sh
WORKDIR /src/
EXPOSE 8080

COPY --from=base /tmp/nta_app/requirements.txt /src/requirements.txt
RUN pip install -r /src/requirements.txt
RUN pip install uwsgi
RUN python --version

ENV PYTHONPATH $PYTHONPATH:/src
ENV PATH $PATH:/src

CMD ["sh", "/src/start_flask.sh"]
