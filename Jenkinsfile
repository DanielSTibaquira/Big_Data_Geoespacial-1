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
    }
}
