# Makefile

phony: ch-create, dev-up, dev-down, test

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

test:
	zig build test -Dopenssl=true \
  -Dopenssl_lib_path=/opt/homebrew/opt/openssl@3/lib \
  -Dopenssl_include_path=/opt/homebrew/opt/openssl@3/include
