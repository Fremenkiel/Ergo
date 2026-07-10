# Makefile

phony: db-create, cl-create, dev-up, dev-down

ADMIN_DB_URL ?= postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable
MAIN_DB_URL ?= postgres://admin:admin@localhost:5432/db?sslmode=disable

CL_ADDR ?= localhost
CL_PORT ?= 9000
CL_USER ?= default
CL_PASS ?= clickhouse
CL_DB ?= audit_log

db-create:
	psql "$(ADMIN_DB_URL)" -v ON_ERROR_STOP=1 -f db/init.sql
	psql "$(MAIN_DB_URL)" -v ON_ERROR_STOP=1 -f db/seed.sql

cl-create:
	clickhouse-client \
  --host "$(CL_ADDR)" \
  --port "$(CL_PORT)" \
  --user "$(CL_USER)" \
  --password "$(CL_PASS)" \
	--database "$(CL_DB)" \
	--queries-file cl/init.sql

dev-up: 
	docker-compose -f ./compose.yml up -d --wait
	make db-create
	make cl-create

dev-down:
	docker-compose -f ./compose.yml down -v
