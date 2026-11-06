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
            defaultValue: 'parcial',
            description: 'Existing Compose project that Jenkins will update.'
        )
    }

    environment {
        PYTHONDONTWRITEBYTECODE = '1'
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
            when {
                branch 'main'
            }
            steps {
                withCredentials([file(credentialsId: 'kaggle-json', variable: 'KAGGLE_JSON')]) {
                    sh '''
                        set -eu
                        config_dir="$WORKSPACE/data/.kaggle-ci"
                        mkdir -p "$config_dir"
                        trap 'rm -rf "$config_dir"' EXIT
                        cp "$KAGGLE_JSON" "$config_dir/kaggle.json"
                        chmod 600 "$config_dir/kaggle.json"

                        compose() {
                            docker-compose \
                                --project-name "$COMPOSE_PROJECT_NAME" \
                                --file "$WORKSPACE/docker-compose.yml" \
                                "$@"
                        }
                        compose --profile jobs up --detach \
                            mongodb dask-scheduler dask-worker-1 dask-worker-2 \
                            spark-master spark-worker
                        compose --profile jobs run --build --rm yelp-ingest \
                            --report /data/yelp_ingestion_report.json
                    '''
                }
            }
        }

        stage('Spark sentiment and closure analytics') {
            when {
                branch 'main'
            }
            steps {
                sh '''
                    set -eu
                    compose() {
                        docker-compose \
                            --project-name "$COMPOSE_PROJECT_NAME" \
                            --file "$WORKSPACE/docker-compose.yml" \
                            "$@"
                    }
                    compose exec -T spark-master sh -c 'mkdir -p /tmp/spark-ivy/cache'
                    compose exec -T spark-master \
                        /opt/spark/bin/spark-submit \
                        --master spark://spark-master:7077 \
                        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 \
                        --conf spark.jars.ivy=/tmp/spark-ivy \
                        --conf spark.mongodb.read.connection.uri=mongodb://mongodb:27017 \
                        --conf spark.mongodb.write.connection.uri=mongodb://mongodb:27017 \
                        /opt/spark-apps/jobs/lifemap_batch.py \
                        --minimum-days-business 365 \
                        --report /lifemap/batch_report.json
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
                    export GIT_COMMIT="$(git rev-parse HEAD)"
                    compose --profile ci-deploy up --detach \
                        mongodb dask-scheduler dask-worker-1 dask-worker-2 \
                        spark-master spark-worker api-ci
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
        if payload.get("status") == "ok":
            print("Candidate API health check passed")
            break
        last_error = f"Unexpected health response: {payload!r}"
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        last_error = str(error)
    time.sleep(2)
else:
    raise SystemExit(f"Candidate API health check failed: {last_error}")

polygon = {
    "type": "Polygon",
    "coordinates": [[
        [-8.70, 41.10],
        [-8.50, 41.10],
        [-8.60, 41.30],
        [-8.70, 41.10],
    ]],
}
request = urllib.request.Request(
    "http://api-ci:5000/api/v1/trips/within",
    data=json.dumps({"polygon": polygon, "limit": 3}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = json.load(response)
except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
    raise SystemExit(f"Candidate GeoJSON query failed: {error}") from error

results = payload.get("results") if isinstance(payload, dict) else None
count = payload.get("count") if isinstance(payload, dict) else None
if not isinstance(results, list) or count != len(results) or count > 3:
    raise SystemExit(f"Unexpected GeoJSON query response: {payload!r}")
print(f"Candidate $geoWithin POST smoke test passed ({count} results)")

with urllib.request.urlopen(
    "http://api-ci:5000/businesses/near"
    "?lon=-75.16&lat=39.95&radius_m=1000&limit=3",
    timeout=15,
) as response:
    payload = json.load(response)
if not isinstance(payload.get("results"), list) or payload.get("count") > 3:
    raise SystemExit(f"Unexpected Yelp API response: {payload!r}")
print(f"Candidate Yelp API smoke test passed ({payload['count']} results)")

yelp_polygon = {
    "type": "Polygon",
    "coordinates": [[
        [-75.35, 39.80],
        [-74.95, 39.80],
        [-74.95, 40.10],
        [-75.35, 40.10],
        [-75.35, 39.80],
    ]],
}
request = urllib.request.Request(
    "http://api-ci:5000/businesses/within",
    data=json.dumps({"polygon": yelp_polygon, "limit": 3}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=15) as response:
    payload = json.load(response)
if not isinstance(payload.get("results"), list) or payload.get("count") > 3:
    raise SystemExit(f"Unexpected Yelp polygon response: {payload!r}")
print(f"Candidate Yelp $geoWithin smoke test passed ({payload['count']} results)")

with urllib.request.urlopen(
    "http://api-ci:5000/zones/summary"
    "?lon=-75.16&lat=39.95&radius_m=5000&min_days_business=365",
    timeout=15,
) as response:
    payload = json.load(response)
if "closure_rate_pct" not in payload or "average_sentiment" not in payload:
    raise SystemExit(f"Unexpected Yelp zone summary: {payload!r}")
print("Candidate Yelp zone summary smoke test passed")

with urllib.request.urlopen(
    "http://api-ci:5000/stats/closure"
    "?category=Restaurants&min_days_business=365",
    timeout=15,
) as response:
    payload = json.load(response)
if not isinstance(payload.get("results"), list):
    raise SystemExit(f"Unexpected Yelp closure response: {payload!r}")
print(f"Candidate Yelp closure stats smoke test passed ({payload['count']} groups)")

if os.environ.get("BRANCH_NAME") == "main":
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
    if set(payload.get("probabilities", {})) != {
        "negative",
        "neutral",
        "positive",
    }:
        raise SystemExit(f"Unexpected Yelp prediction probabilities: {payload!r}")
    print("Candidate Yelp sentiment prediction smoke test passed")
PY
                    cleanup_candidate
                    trap - EXIT
                '''
            }
        }

        stage('Deploy API') {
            when {
                branch 'main'
            }
            steps {
                sh '''
                    set -eu
                    export GIT_COMMIT="$(git rev-parse HEAD)"
                    docker-compose \
                        --project-name "$COMPOSE_PROJECT_NAME" \
                        --file "$WORKSPACE/docker-compose.yml" \
                        up --detach --no-deps api
                '''
            }
        }
    }
}
