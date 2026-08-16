# Makefile

phony: ch-create, dev-up, dev-down, db-up, db-down, ch-up, ch-down, test, test-ch, test-pg, build, run, wal-logger, rebuld-db

CH_ADDR ?= localhost
CH_PORT ?= 9000
CH_USER ?= default
CH_PASS ?= clickhouse
CH_DB ?= audit_log

ch-create:
	clickhouse-client \
  --host "$(CH_ADDR)" \
  --port "$(CH_PORT)" \
  --user "$(CH_USER)" \
  --password "$(CH_PASS)" \
	--database "$(CH_DB)" \
	--queries-file infra/ch/init.sql

dev-up: 
	docker-compose -f ./infra/compose.yml up -d --wait
	make ch-create

dev-down:
	docker-compose -f ./infra/compose.yml down -v

db-up: 
	docker-compose -f ./infra/compose.yml up db -d --wait

db-down:
	docker-compose -f ./infra/compose.yml down db -v

ch-up: 
	docker-compose -f ./infra/compose.yml up -d ch --wait
	make ch-create

ch-down:
	docker-compose -f ./infra/compose.yml down ch -v

test:
	zig build test \
  -Dopenssl_lib_path=/opt/homebrew/opt/openssl@3/lib \
  -Dopenssl_include_path=/opt/homebrew/opt/openssl@3/include \
	-freference-trace=10

test-ch:
	zig build ch_test \
	-freference-trace=10

test-pg:
	zig build pg_test \
  -Dopenssl_lib_path=/opt/homebrew/opt/openssl@3/lib \
  -Dopenssl_include_path=/opt/homebrew/opt/openssl@3/include \
	-freference-trace=10

build:
	zig build \
  -Dopenssl_lib_path=/opt/homebrew/opt/openssl@3/lib \
  -Dopenssl_include_path=/opt/homebrew/opt/openssl@3/include

run:
	zig build run \
  -Dopenssl_lib_path=/opt/homebrew/opt/openssl@3/lib \
  -Dopenssl_include_path=/opt/homebrew/opt/openssl@3/include

wal-logger:
	zig build wal-logger \
	-Dopenssl_lib_path=/opt/homebrew/opt/openssl@3/lib \
  -Dopenssl_include_path=/opt/homebrew/opt/openssl@3/include \
	-freference-trace=10

rebuld-db:
	docker compose -f infra/compose.yml build db
