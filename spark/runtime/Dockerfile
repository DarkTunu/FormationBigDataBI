FROM maven:3.9.16-eclipse-temurin-17 AS dependency-resolver

WORKDIR /build
COPY spark/runtime/pom.xml /build/pom.xml

# Resolve normal runtime dependencies first.  The POM contains ONE AWS SDK
# bundle and explicitly excludes Hadoop's transitive AWS bundle.
RUN mvn -B -q -f /build/pom.xml dependency:copy-dependencies \
      -DincludeScope=runtime \
      -DoutputDirectory=/deps

# Iceberg S3FileIO is configured to use URLConnection.  Add ONLY that HTTP
# implementation JAR, without its transitive modular AWS SDK dependencies.
RUN mvn -B -q \
      org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
      -Dartifact=software.amazon.awssdk:url-connection-client:2.44.4 \
      -DoutputDirectory=/deps

# Build-time dependency contract.
RUN set -eux; \
    test -f /deps/delta-spark_2.13-4.0.0.jar; \
    test -f /deps/iceberg-spark-runtime-4.0_2.13-1.11.0.jar; \
    test -f /deps/hadoop-aws-3.4.1.jar; \
    test -f /deps/spark-sql-kafka-0-10_2.13-4.0.4.jar; \
    test -f /deps/bundle-2.44.4.jar; \
    test -f /deps/url-connection-client-2.44.4.jar; \
    for pattern in \
      'sdk-core-*.jar' \
      'aws-core-*.jar' \
      'utils-*.jar' \
      'auth-*.jar' \
      'profiles-*.jar' \
      'regions-*.jar' \
      'protocol-core-*.jar' \
      'http-client-spi-*.jar' \
      'identity-spi-*.jar' \
      'retries-*.jar' \
      'retries-spi-*.jar' \
      'endpoints-spi-*.jar'; do \
        if find /deps -maxdepth 1 -type f -name "$pattern" -print -quit | grep -q .; then \
          echo "ERROR: modular AWS SDK core jar detected: $pattern" >&2; \
          find /deps -maxdepth 1 -type f -name "$pattern" -print >&2; \
          exit 41; \
        fi; \
    done; \
    sources="$(for j in /deps/*.jar; do \
      if jar tf "$j" | grep -qx 'software/amazon/awssdk/utils/IoUtils.class'; then echo "$j"; fi; \
    done)"; \
    printf '%s\n' "$sources"; \
    test "$(printf '%s\n' "$sources" | sed '/^$/d' | wc -l)" -eq 1; \
    javap -classpath '/deps/*' software.amazon.awssdk.utils.IoUtils \
      | grep -q 'software.amazon.awssdk.thirdparty.org.slf4j.Logger'

FROM apache/spark:4.0.4-scala2.13-java17-python3-r-ubuntu

# Retry slow package-index downloads during clean parallel image builds.
ENV PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10

USER root

# PySpark driver/workers must share the same Python minor version.  The exact
# minor is discovered at runtime and later compared with the Airflow driver.
RUN python3 -c "import sys; print('SPARK_PYTHON_OK=' + '.'.join(map(str, sys.version_info[:2])))"

# Native build/runtime prerequisites for the requested notebook libraries.
# - psycopg2 needs pg_config/libpq headers when pip builds from source.
# - pygraphviz needs Graphviz headers, pkg-config and a C compiler.
# The packages remain in the image so both Python modules keep their native
# shared-library dependencies at notebook runtime.
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential \
      graphviz \
      libgraphviz-dev \
      libpq-dev \
      pkg-config \
      python3-cairo \
      python3-dev; \
    rm -rf /var/lib/apt/lists/*; \
    command -v pg_config; \
    command -v dot; \
    pkg-config --exists libgvc

# Python dependencies required by the training ML jobs.
# PySpark SQL can run without NumPy, but importing pyspark.ml requires it.
COPY spark/runtime/requirements-ml.txt /tmp/requirements-ml.txt
COPY spark/runtime/requirements-jupyter.txt /tmp/requirements-jupyter.txt
COPY spark/runtime/validate_python_dependencies.py /tmp/validate_python_dependencies.py
RUN set -eux; \
    python3 -m pip install --no-cache-dir -r /tmp/requirements-ml.txt; \
    python3 -m pip install --no-cache-dir -r /tmp/requirements-jupyter.txt; \
    export PYTHONPATH="/opt/spark/python:$(echo /opt/spark/python/lib/py4j-*-src.zip)"; \
    python3 /tmp/validate_python_dependencies.py; \
    python3 -m pip check; \
    python3 -m jupyter lab --version; \
    rm -f /tmp/requirements-ml.txt /tmp/requirements-jupyter.txt /tmp/validate_python_dependencies.py

# The upstream Spark user has /nonexistent as its passwd home. Jupyter needs a
# writable home/runtime tree, otherwise it exits before opening port 8888.
RUN set -eux; \
    install -d -m 0755 -o spark -g spark /home/spark; \
    install -d -m 1777 /tmp/spark-ivy /tmp/spark-ivy/cache /tmp/spark-ivy/jars; \
    install -d -m 0700 -o spark -g spark \
      /home/spark/.config \
      /home/spark/.cache \
      /home/spark/.jupyter \
      /home/spark/.local/share/jupyter/runtime; \
    chown -R spark:spark /home/spark; \
    chmod 0700 \
      /home/spark/.config \
      /home/spark/.cache \
      /home/spark/.jupyter \
      /home/spark/.local \
      /home/spark/.local/share \
      /home/spark/.local/share/jupyter \
      /home/spark/.local/share/jupyter/runtime; \
    test -z "$(find /home/spark \( ! -user spark -o ! -group spark \) -print -quit)"; \
    py4j_zip="$(find /opt/spark/python/lib -maxdepth 1 -type f -name 'py4j-*-src.zip' -print -quit)"; \
    test -n "$py4j_zip"; \
    ln -sf "$py4j_zip" /opt/spark/python/lib/py4j-src.zip; \
    chown -h spark:spark /opt/spark/python/lib/py4j-src.zip

ENV HOME=/home/spark \
    XDG_CONFIG_HOME=/home/spark/.config \
    XDG_CACHE_HOME=/home/spark/.cache \
    XDG_DATA_HOME=/home/spark/.local/share \
    JUPYTER_CONFIG_DIR=/home/spark/.jupyter \
    JUPYTER_DATA_DIR=/home/spark/.local/share/jupyter \
    JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-src.zip

# Validate the exact import path used by a normal Python/Jupyter process. This
# is deliberately not executed through spark-submit or the Spark entrypoint.
RUN python3 -c "import py4j, pyspark; print('JUPYTER_PYSPARK_IMPORT_OK pyspark=' + pyspark.__version__ + ' py4j=' + py4j.__version__)"


# The upstream Spark image must not already contain a competing AWS SDK IoUtils.
RUN set -eux; \
    for j in /opt/spark/jars/*.jar; do \
      if jar tf "$j" 2>/dev/null | grep -qx 'software/amazon/awssdk/utils/IoUtils.class'; then \
        echo "ERROR: upstream Spark image already contains AWS SDK IoUtils in $j" >&2; \
        exit 42; \
      fi; \
    done

COPY --from=dependency-resolver /deps/ /opt/spark/jars/

# Final classpath assertion: exactly one IoUtils provider, from the AWS bundle.
RUN set -eux; \
    chown -R spark:spark /opt/spark/jars; \
    sources="$(for j in /opt/spark/jars/*.jar; do \
      if jar tf "$j" 2>/dev/null | grep -qx 'software/amazon/awssdk/utils/IoUtils.class'; then echo "$j"; fi; \
    done)"; \
    printf '%s\n' "$sources"; \
    test "$(printf '%s\n' "$sources" | sed '/^$/d' | wc -l)" -eq 1; \
    echo "$sources" | grep -q '/bundle-2.44.4.jar$'; \
    javap -classpath '/opt/spark/jars/*' software.amazon.awssdk.utils.IoUtils \
      | grep -q 'software.amazon.awssdk.thirdparty.org.slf4j.Logger'; \
    test -f /opt/spark/jars/url-connection-client-2.44.4.jar; \
    export PYTHONPATH="/opt/spark/python:$(echo /opt/spark/python/lib/py4j-*-src.zip)"; \
    python3 -c "import numpy as np; from pyspark.ml.clustering import KMeans; from pyspark.ml.feature import StandardScaler, VectorAssembler; print('FINAL_SPARK_ML_PYTHON_OK numpy=' + np.__version__)"

USER spark

USER root
RUN mkdir -p /home/spark /opt/spark/donnees /opt/spark/jobs \
    && ln -s /opt/spark/donnees /home/spark/donnees \
    && ln -s /opt/spark/jobs /home/spark/jobs
USER spark
