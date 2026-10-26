FROM ubuntu:24.04@sha256:68ee22004a9109b44973313b02ff7b1de250f1de0f5ecb628d73f579aaff33ac
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential clang cmake ninja-build git ca-certificates libtbb-dev \
    && rm -rf /var/lib/apt/lists/*
ENV BENCH_NUM_THREADS=8
WORKDIR /src
COPY run.sh /opt/repro/run.sh
ENTRYPOINT ["bash", "/opt/repro/run.sh"]
