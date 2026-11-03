FROM ubuntu:24.04@sha256:68ee22004a9109b44973313b02ff7b1de250f1de0f5ecb628d73f579aaff33ac
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential clang cmake ninja-build git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch v2022.3.0 https://github.com/uxlfoundation/oneTBB.git /tmp/oneTBB \
    && test "$(git -C /tmp/oneTBB rev-parse HEAD)" = f1862f38f83568d96e814e469ab61f88336cc595 \
    && cmake -S /tmp/oneTBB -B /tmp/tbb-build -G Ninja \
         -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=20 -DTBB_TEST=OFF \
    && cmake --build /tmp/tbb-build --parallel 4 \
    && cmake --install /tmp/tbb-build \
    && ldconfig \
    && rm -rf /tmp/oneTBB /tmp/tbb-build
ENV BENCH_NUM_THREADS=8
WORKDIR /src
COPY run.sh /opt/repro/run.sh
ENTRYPOINT ["bash", "/opt/repro/run.sh"]
