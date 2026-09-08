# Builds this modified Konclude (see README.md) into a small, fully
# self-contained image -- runs identically on Windows (via Docker Desktop),
# Linux, and macOS, regardless of what's installed on the host. This is the
# actual fix for "not dependent on the environment": rather than chasing a
# statically-linked binary per platform, the container always carries its
# own exact runtime with it.
#
# Built WITH Redland (KoncludeRedlandLinux.pro, against system-installed
# Redland/Raptor/Rasqal via pkg-config), unlike our Windows build --
# upstream Konclude's own .pro expects prebuilt Redland static libraries
# under External/librdf/ that our checkout never had, so on Windows we
# built without them (KoncludeWithoutRedland.pro). On Linux those libraries
# are just an apt-get away, so this build reads Turtle/RDF/N-Triples
# directly -- no separate conversion step needed, confirmed by actually
# running it against a real .ttl file (not just a successful compile).
# We still never use Konclude's own SPARQL/Rasqal query engine for
# reasoning -- only its Redland-based *parser* -- everything materialize
# computes is still our own native code, same as the Windows build.
#
# Build:  docker build -t konclude .
# Run:    docker run --rm -v "$(pwd):/data" konclude materialize -w AUTO -i /data/your-ontology.ttl -o /data/output.ttl
#         (input/output can be Turtle, RDF/XML, N-Triples, OWL2-XML, or OWL2-Functional -- format is auto-detected)

# ---- Stage 1: build ----
FROM ubuntu:22.04 AS builder

RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential qtbase5-dev qt5-qmake pkg-config \
        librdf0-dev libraptor2-dev librasqal3-dev libxml2-dev libjemalloc-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/include/redland.h /usr/include/Redland.h

WORKDIR /src
COPY . .

RUN qmake KoncludeRedlandLinux.pro && \
    make -j"$(nproc)"

# ---- Stage 2: runtime ----
FROM ubuntu:22.04

RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        libqt5core5a libqt5network5 libqt5xml5 libqt5concurrent5 \
        librdf0 libraptor2-0 librasqal3 libxml2 libjemalloc2 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/Release/Konclude /usr/local/bin/Konclude
COPY Configs /opt/konclude/Configs

WORKDIR /data
ENTRYPOINT ["/usr/local/bin/Konclude"]
CMD ["-h"]
