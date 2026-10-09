FROM jenkins/jenkins:lts-jdk17

RUN jenkins-plugin-cli --plugins github workflow-aggregator credentials-binding git

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        docker-cli \
        docker-compose \
        python3 \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/jenkins-requirements.txt
RUN python3 -m venv /opt/kaggle-venv \
    && /opt/kaggle-venv/bin/pip install --no-cache-dir -r /tmp/jenkins-requirements.txt \
    && rm /tmp/jenkins-requirements.txt

ENV PATH="/opt/kaggle-venv/bin:${PATH}"

USER jenkins
