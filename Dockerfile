FROM python:3.9 AS base

ARG BRANCH_NAME=dev

RUN apt-get update --allow-releaseinfo-change -y
RUN apt-get upgrade --fix-missing -y
RUN apt-get install -y --fix-missing --no-install-recommends git


RUN cd /tmp && git clone -b ${BRANCH_NAME} https://github.com/quanted/nta_app.git
#RUN cd /tmp && git clone -b dev https://github.com/quanted/nta_app.git # use if working on a non-dev feature branch

FROM continuumio/miniconda3:4.10.3

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

RUN conda create --name pyenv python=3.9
RUN conda config --add channels conda-forge
RUN conda run -n pyenv --no-capture-output pip install -r /src/nta_flask/requirements.txt
RUN conda install -n pyenv uwsgi

ENV PYTHONPATH /src:/src/nta_flask/:$PYTHONPATH
ENV PATH /src:/src/nta_flask/:$PATH

CMD ["conda", "run", "-n", "pyenv", "--no-capture-output", "sh", "/src/nta_flask/start_flask.sh"]
