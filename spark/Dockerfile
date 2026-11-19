FROM apache/spark:3.5.7

USER root
RUN python3 -m pip install --no-cache-dir numpy==1.24.4 "pymongo>=4.10,<5" \
    && mkdir -p /tmp/spark-ivy/cache \
    && chown -R spark:spark /tmp/spark-ivy
USER spark
