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
        string(
            name: 'COMPOSE_PROJECT_NAME',
            defaultValue: 'big_data_geoespacial-1',
            description: 'Existing Compose project that Jenkins will update.'
        )
    }

    environment {
        PYTHONDONTWRITEBYTECODE = '1'
        BRANCH_NAME = 'main'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
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

        stage('Yelp ingestion') {
            when { expression { false } }
            steps {
                echo 'Skipping redundant Kaggle ingestion (already persisted).'
            }
        }

        stage('Spark sentiment and closure analytics') {
            when { expression { false } }
            steps {
                echo 'Skipping redundant E1 batch analytics (already persisted).'
            }
        }

        stage('E2 temporal sentiment model') {
            when {
                environment name: 'BRANCH_NAME', value: 'main'
            }
            steps {
                sh '''
                    set -eu
                    docker compose --profile e2 \
                        --project-name "$COMPOSE_PROJECT_NAME" \
                        --file "$WORKSPACE/docker-compose.yml" \
                        exec -T spark-master test -f /lifemap/model_serving_as_of_2021-01-01.json
                    echo "Verified versioned temporal sentiment model artifact (/lifemap/model_serving_as_of_2021-01-01.json)"
                '''
            }
        }

        stage('E2 survival and risk model') {
            when {
                environment name: 'BRANCH_NAME', value: 'main'
            }
            steps {
                sh '''
                    set -eu
                    docker compose --profile e2 \
                        --project-name "$COMPOSE_PROJECT_NAME" \
                        --file "$WORKSPACE/docker-compose.yml" \
                        exec -T spark-master test -f /lifemap/risk_model_as_of_2021-01-01.json
                    echo "Verified survival and risk model artifact (/lifemap/risk_model_as_of_2021-01-01.json)"
                '''
            }
        }

        stage('Build API image') {
            steps {
                sh '''
                    set -eu
                    docker compose --profile e2 \
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
                        docker compose --profile e2 \
                            --project-name "$COMPOSE_PROJECT_NAME" \
                            --file "$WORKSPACE/docker-compose.yml" \
                            "$@"
                    }
                    cleanup_candidate() {
                        compose --profile ci-deploy rm --force --stop api-ci
                    }
                    trap cleanup_candidate EXIT
                    export GIT_COMMIT="$(git rev-parse HEAD)"
                    export LIFEMAP_STREAMING_ENABLED=true
                    compose --profile ci-deploy up --detach --no-recreate \
                        mongodb kafka dask-scheduler dask-worker-1 dask-worker-2 \
                        spark-master spark-worker lifemap-streaming
                    compose --profile ci-deploy up --detach --force-recreate --no-deps api-ci

                    compose --profile jobs run --rm --no-deps \
                        replay-reviews --rate 1000 --limit 100

                    python3 - <<'PY'
import json
import os
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
        if (
            payload.get("status") == "ok"
            and payload.get("commit") == os.environ.get("GIT_COMMIT")
        ):
            print("Candidate API health check passed")
            break
        last_error = f"Unexpected health response: {payload!r}"
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        last_error = str(error)
    time.sleep(2)
else:
    raise SystemExit(f"Candidate API health check failed: {last_error}")

with urllib.request.urlopen(
    "http://api-ci:5000/survival/Restaurants", timeout=15
) as response:
    payload = json.load(response)
if not isinstance(payload.get("points"), list):
    raise SystemExit(f"Unexpected survival curve response: {payload!r}")
print("Candidate Kaplan-Meier API smoke test passed")

for endpoint in (
    "http://api-ci:5000/live/hotspots?lon=-75.16&lat=39.95&radius_m=25000&window_days=30",
    "http://api-ci:5000/alerts?lon=-75.16&lat=39.95&radius_m=25000",
    "http://api-ci:5000/risk/near?lon=-75.16&lat=39.95&radius_m=25000",
):
    end_t = time.monotonic() + 30
    while time.monotonic() < end_t:
        try:
            with urllib.request.urlopen(endpoint, timeout=15) as response:
                payload = json.load(response)
            if isinstance(payload.get("results"), list):
                break
        except urllib.error.HTTPError as err:
            if err.code != 503:
                raise
            time.sleep(2)
    else:
        raise SystemExit(f"Endpoint remained 503: {endpoint}")
print("Candidate streaming & risk endpoints smoke test passed")

request = urllib.request.Request(
    "http://api-ci:5000/predict",
    data=json.dumps({"text": "Great food and friendly service"}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=15) as response:
    payload = json.load(response)
if payload.get("sentiment") not in {"negative", "neutral", "positive"}:
    raise SystemExit(f"Unexpected Yelp prediction response: {payload!r}")
print("Candidate Yelp sentiment prediction smoke test passed")

deadline = time.monotonic() + 120
last_payload = None
while time.monotonic() < deadline:
    with urllib.request.urlopen("http://api-ci:5000/health", timeout=5) as response:
        last_payload = json.load(response)
    services = last_payload.get("services", {})
    lag = last_payload.get("consumer_lag", {}).get("messages")
    if services.get("kafka") == "ok" and services.get("streaming") == "ok" and lag == 0:
        print("Kafka-to-Mongo streaming smoke test passed")
        break
    time.sleep(2)
else:
    raise SystemExit(f"Streaming did not drain the replayed messages: {last_payload!r}")
PY
                    compose exec -T api-ci python3 -c "import json; d=json.load(open('/lifemap/model_serving_as_of_2021-01-01.json')); c=str(d.get('cutoff') or d.get('train_cutoff') or ''); assert '2021-01-01' in c, c; print('Versioned sentiment model check passed (cutoff 2021-01-01)')"
                    features_count=$(compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('lifemap').risk_features.countDocuments({})")
                    hotspots_count=$(compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('lifemap').live_hotspots.countDocuments({})")
                    echo "MongoDB streaming collections -> risk_features: $features_count, live_hotspots: $hotspots_count"
                    if [ "$features_count" -lt 1 ]; then
                        echo "Expected streaming features classified in MongoDB, found $features_count"
                        exit 1
                    fi
                    cleanup_candidate
                    trap - EXIT
                '''
            }
        }

        stage('Deploy API') {
            when {
                environment name: 'BRANCH_NAME', value: 'main'
            }
            steps {
                sh '''
                    set -eu
                    export GIT_COMMIT="$(git rev-parse HEAD)"
                    export LIFEMAP_STREAMING_ENABLED=true
                    docker compose --profile e2 \
                        --project-name "$COMPOSE_PROJECT_NAME" \
                        --file "$WORKSPACE/docker-compose.yml" \
                        up --detach --no-deps api
                '''
            }
        }
    }
}