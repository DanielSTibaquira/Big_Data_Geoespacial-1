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
                    compose up --build --force-recreate --detach \
                        spark-master spark-worker
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

        stage('E2 temporal sentiment model') {
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
                        --executor-memory 1536m \
                        --conf spark.executor.cores=1 \
                        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 \
                        --conf spark.jars.ivy=/tmp/spark-ivy \
                        --conf spark.mongodb.read.connection.uri=mongodb://mongodb:27017 \
                        --conf spark.mongodb.write.connection.uri=mongodb://mongodb:27017 \
                        /opt/spark-apps/jobs/lifemap_batch.py \
                        --minimum-days-business 365 \
                        --sentiment-train-cutoff 2021-01-01T00:00:00Z \
                        --skip-benchmark \
                        --model-dir /lifemap/models_as_of_2021-01-01 \
                        --serving-model /lifemap/model_serving_as_of_2021-01-01.json \
                        --report /lifemap/batch_report_as_of_2021-01-01.json
                '''
            }
        }

        stage('E2 survival and risk model') {
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
                        --conf spark.driver.host=spark-master \
                        --conf spark.driver.bindAddress=0.0.0.0 \
                        /opt/spark-apps/jobs/lifemap_e2_analytics.py \
                        --cutoff 2021-01-01T00:00:00Z \
                        --risk-model /lifemap/risk_model_as_of_2021-01-01.json \
                        --report /lifemap/e2_analytics_report.json
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
                    if [ "$BRANCH_NAME" = main ]; then
                        export LIFEMAP_STREAMING_ENABLED=true
                        compose --profile e2 --profile ci-deploy up --detach \
                            mongodb kafka dask-scheduler dask-worker-1 dask-worker-2 \
                            spark-master spark-worker lifemap-streaming api-ci
                    else
                        compose --profile ci-deploy up --detach \
                            mongodb dask-scheduler dask-worker-1 dask-worker-2 \
                            spark-master spark-worker api-ci
                    fi
                    if [ "$BRANCH_NAME" = main ]; then
                        compose --profile e2 --profile jobs run --build --rm \
                            replay-reviews --rate 1000 --limit 100
                    fi
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
    with urllib.request.urlopen(
        "http://api-ci:5000/survival/Restaurants", timeout=15
    ) as response:
        payload = json.load(response)
    if not isinstance(payload.get("points"), list):
        raise SystemExit(f"Unexpected survival curve response: {payload!r}")
    print("Candidate Kaplan-Meier API smoke test passed")

    with urllib.request.urlopen(
        "http://api-ci:5000/live/hotspots"
        "?lon=-75.16&lat=39.95&radius_m=5000&window_days=30",
        timeout=15,
    ) as response:
        payload = json.load(response)
    if not isinstance(payload.get("results"), list):
        raise SystemExit(f"Unexpected hotspot response: {payload!r}")

    with urllib.request.urlopen(
        "http://api-ci:5000/alerts?lon=-75.16&lat=39.95&radius_m=5000",
        timeout=15,
    ) as response:
        payload = json.load(response)
    if not isinstance(payload.get("results"), list):
        raise SystemExit(f"Unexpected alert response: {payload!r}")

    with urllib.request.urlopen(
        "http://api-ci:5000/risk/near?lon=-75.16&lat=39.95&radius_m=5000",
        timeout=15,
    ) as response:
        payload = json.load(response)
    if not isinstance(payload.get("results"), list):
        raise SystemExit(f"Unexpected risk response: {payload!r}")

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
                    if [ "$BRANCH_NAME" = main ]; then
                        python3 - <<'PY'
import json
import os
import time
import urllib.request

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

model_path = os.path.join(
    os.environ["WORKSPACE"],
    "data",
    "lifemap",
    "model_serving_as_of_2021-01-01.json",
)
with open(model_path, encoding="utf-8") as fh:
    serving_model = json.load(fh)
cutoff = str(serving_model.get("cutoff") or serving_model.get("train_cutoff") or "")
if "2021-01-01" not in cutoff:
    raise SystemExit(f"Expected versioned model cutoff 2021-01-01, got: {cutoff!r}")
print("Versioned sentiment model check passed (cutoff 2021-01-01)")
PY
                        features_count=$(compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('lifemap').risk_features.countDocuments({})")
                        hotspots_count=$(compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('lifemap').live_hotspots.countDocuments({})")
                        echo "MongoDB streaming collections -> risk_features: $features_count, live_hotspots: $hotspots_count"
                        if [ "$features_count" -lt 1 ]; then
                            echo "Expected streaming features classified in MongoDB, found $features_count"
                            exit 1
                        fi
                    fi
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
                    export LIFEMAP_STREAMING_ENABLED=true
                    docker-compose \
                        --project-name "$COMPOSE_PROJECT_NAME" \
                        --file "$WORKSPACE/docker-compose.yml" \
                        --profile e2 \
                        up --detach api lifemap-streaming
                '''
            }
        }
    }
}