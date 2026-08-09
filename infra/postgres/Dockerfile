# STAGE 1: Build the extension
FROM postgres:18.4 AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*

COPY ergo_pgoutput /usr/src/ergo_pgoutput
WORKDIR /usr/src/ergo_pgoutput
RUN make clean && make && make install

# STAGE 2: Create the clean final image
FROM postgres:18.4

COPY --from=builder /usr/lib/postgresql/18/lib/ergo_pgoutput.so /usr/lib/postgresql/18/lib/
