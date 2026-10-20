FROM maven:3.9.16-eclipse-temurin-17 AS dependency-resolver
WORKDIR /build
COPY spark/runtime/pom.xml /build/pom.xml
# Resolve the shared Spark/Iceberg runtime JARs.  The AWS URLConnection HTTP
# implementation is intentionally copied as a single artifact (without pulling
# a second modular AWS SDK dependency tree).  Its version is read from the POM.
RUN set -eux; \
    mvn -B -q -f /build/pom.xml dependency:copy-dependencies \
      -DincludeScope=runtime \
      -DoutputDirectory=/deps; \
    AWS_SDK_VERSION="$(mvn -B -q -f /build/pom.xml help:evaluate -Dexpression=aws.sdk.version -DforceStdout | tail -n 1)"; \
    test -n "$AWS_SDK_VERSION"; \
    mvn -B -q \
      org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
      -Dartifact="software.amazon.awssdk:url-connection-client:${AWS_SDK_VERSION}" \
      -DoutputDirectory=/deps; \
    test -f "/deps/url-connection-client-${AWS_SDK_VERSION}.jar"; \
    jar tf "/deps/url-connection-client-${AWS_SDK_VERSION}.jar" \
      | grep -qx 'software/amazon/awssdk/http/urlconnection/UrlConnectionHttpClient$Builder.class'; \
    mvn -B -q -f /build/pom.xml help:evaluate -Dexpression=spark.version -DforceStdout \
      | tail -n 1 > /spark-version.txt

# PySpark requires the same Python minor version on driver and workers.
# Runtime parity is validated dynamically by the installer.
FROM apache/airflow:3.1.7-python3.10

# Retry slow package-index downloads during clean parallel image builds.
ENV PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && JAVA_BIN="$(readlink -f "$(command -v java)")" \
    && JAVA_HOME_REAL="$(dirname "$(dirname "$JAVA_BIN")")" \
    && ln -s "$JAVA_HOME_REAL" /opt/java-home \
    && rm -rf /var/lib/apt/lists/*

COPY --from=dependency-resolver /deps/ /opt/spark-extra-jars/
COPY --chown=airflow:root --from=dependency-resolver /spark-version.txt /tmp/spark-version.txt
COPY --chown=airflow:root spark/runtime/requirements-ml.txt /tmp/requirements-ml.txt
RUN chown -R airflow:root /opt/spark-extra-jars

ENV JAVA_HOME=/opt/java-home
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV SPARK_DIST_CLASSPATH=/opt/spark-extra-jars/*
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER airflow
# Keep the Airflow environment aligned with the constraint set published for the
# Airflow release already present in the base image.  PySpark is the only
# deliberate override: its version is read from the Spark runtime POM so the
# Airflow driver always matches the Standalone Spark cluster.  Provider 6.x is
# not selected by the Airflow constraints used here, which avoids installing
# pyspark-client on top of the full pyspark package.
RUN set -eux; \
    AIRFLOW_VERSION="$(airflow version | tr -d '[:space:]')"; \
    PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"; \
    SPARK_VERSION="$(tr -d '[:space:]' < /tmp/spark-version.txt)"; \
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"; \
    curl -fsSL "$CONSTRAINT_URL" -o /tmp/constraints.txt; \
    grep -Ev '^(pyspark|pyspark-client)==?' /tmp/constraints.txt > /tmp/constraints-lab.txt; \
    python3 -m pip install --no-cache-dir \
      --constraint /tmp/constraints-lab.txt \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-apache-spark \
      "pyspark==${SPARK_VERSION}" \
      -r /tmp/requirements-ml.txt; \
    rm -f /tmp/requirements-ml.txt /tmp/constraints.txt /tmp/constraints-lab.txt; \
    python3 -m pip check; \
    python3 -c "import importlib.metadata as md; from pathlib import Path; import pyspark; expected=Path('/tmp/spark-version.txt').read_text().strip(); assert pyspark.__version__ == expected, (pyspark.__version__, expected); print('AIRFLOW_PYSPARK_OK=' + pyspark.__version__); print('AIRFLOW_SPARK_PROVIDER_OK=' + md.version('apache-airflow-providers-apache-spark'))"; \
    if python3 -m pip show pyspark-client >/dev/null 2>&1; then echo 'ERROR: pyspark-client must not coexist with full pyspark in this image' >&2; exit 43; else echo 'AIRFLOW_PYSPARK_CLIENT_ABSENT=OK'; fi; \
    python3 -c "import sys, numpy as np; from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator; from pyspark.ml.clustering import KMeans; from pyspark.ml.feature import StandardScaler, VectorAssembler; print('AIRFLOW_SPARK_ML_OK python=' + f'{sys.version_info.major}.{sys.version_info.minor}' + ' numpy=' + np.__version__)"; \
    rm -f /tmp/spark-version.txt

USER root
RUN install -d -m 1777 /tmp/spark-ivy /tmp/spark-ivy/cache /tmp/spark-ivy/jars \
    && mkdir -p /home/spark /opt/spark/donnees /opt/spark/jobs \
    && ln -s /opt/spark/donnees /home/spark/donnees \
    && ln -s /opt/spark/jobs /home/spark/jobs
USER airflow
