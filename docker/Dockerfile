FROM centos:7

RUN yum makecache fast && yum -y install epel-release \
    && yum -y install https://centos7.iuscommunity.org/ius-release.rpm \
    && yum -y install \
        python36u \
        python36u-devel \
        python36u-pip \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN mkdir -p /srv/src

# setup virtual env and activate it for interactive shell
RUN python3.6 -m venv /srv/venv \
    && . /srv/venv/bin/activate \
    && pip install -U --no-cache-dir pip setuptools \
    && echo "source /srv/venv/bin/activate" >> ~/.bashrc

ENV PATH="/srv/venv/bin:${PATH}"
ENV LANG="en_US.utf-8"

WORKDIR /srv/

COPY reqs /srv/reqs
RUN . /srv/venv/bin/activate && pip install -U --no-cache-dir -r /srv/reqs/requirements.txt

RUN mkdir -p /srv/src/
COPY src/ /srv/src/
