pipeline {
    agent any

    options {
        skipDefaultCheckout(true)
        timestamps()
    }

    triggers {
        githubPush()
    }

    parameters {
        booleanParam(
            name: 'FORCE_DATASET_DOWNLOAD',
            defaultValue: false,
            description: 'Download the Kaggle dataset again even when /data/train.csv exists.'
        )
        string(
            name: 'COMPOSE_PROJECT_NAME',
            defaultValue: 'parcial',
            description: 'Existing Compose project that Jenkins will update.'
        )
    }

    environment {
        KAGGLE_DATASET = 'crailtap/taxi-trajectory'
        PYTHONDONTWRITEBYTECODE = '1'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Download Kaggle dataset') {
            steps {
                withCredentials([file(credentialsId: 'kaggle-json', variable: 'KAGGLE_JSON')]) {
                    sh '''
                        set -eu
                        if [ "${FORCE_DATASET_DOWNLOAD:-false}" = "true" ] || [ ! -s /data/train.csv ]; then
                            config_dir="$(mktemp -d)"
                            trap 'rm -rf "$config_dir"' EXIT
                            mkdir -p /data
                            cp "$KAGGLE_JSON" "$config_dir/kaggle.json"
                            chmod 600 "$config_dir/kaggle.json"
                            export KAGGLE_CONFIG_DIR="$config_dir"
                            kaggle datasets download "$KAGGLE_DATASET" \
                                --path /data \
                                --unzip \
                                --force
                        else
                            echo 'Using existing /data/train.csv'
                        fi
                        test -s /data/train.csv
                    '''
                }
            }
        }

        stage('Unit tests') {
            steps {
                sh '''
                    set -eu
                    python3 -m venv .venv
                    .venv/bin/pip install --disable-pip-version-check \
                        -r ingestion/requirements.txt \
                        -r api/requirements.txt \
                        -r requirements-dev.txt
                    .venv/bin/python -m pytest -q
                '''
            }
        }

        stage('Build API image') {
            steps {
                sh '''
                    set -eu
                    docker-compose \
                        --project-name "$COMPOSE_PROJECT_NAME" \
                        --file "$WORKSPACE/docker-compose.yml" \
                        build api
                '''
            }
        }

        stage('Candidate deployment and API smoke test') {
            steps {
                sh '''
                    set -eu
                    compose() {
                        docker-compose \
                            --project-name "$COMPOSE_PROJECT_NAME" \
                            --file "$WORKSPACE/docker-compose.yml" \
                            "$@"
                    }
                    cleanup_candidate() {
                        compose --profile ci-deploy rm --force --stop api-ci
                    }
                    trap cleanup_candidate EXIT
                    compose --profile ci-deploy up --detach mongodb api-ci
                    python3 - <<'PY'
import json
import time
import urllib.error
import urllib.request

deadline = time.monotonic() + 60
last_error = "No response from the candidate API"
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen(
            "http://api-ci:5000/health", timeout=3
        ) as response:
            payload = json.load(response)
        if payload.get("status") == "ok":
            print("Candidate API health check passed")
            break
        last_error = f"Unexpected health response: {payload!r}"
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        last_error = str(error)
    time.sleep(2)
else:
    raise SystemExit(f"Candidate API health check failed: {last_error}")
PY
                    cleanup_candidate
                    trap - EXIT
                '''
            }
        }

        stage('Deploy API') {
            steps {
                sh '''
                    set -eu
                    docker-compose \
                        --project-name "$COMPOSE_PROJECT_NAME" \
                        --file "$WORKSPACE/docker-compose.yml" \
                        up --detach --no-deps api
                '''
            }
        }
    }
}
