init:
	docker-compose up airflow-init
start:
	-mkdir plugins
	-mkdir logs
	docker-compose up -d

debug:
	-mkdir plugins
	-mkdir logs
	docker-compose up

stop:
	docker-compose down

init-local:
	docker-compose -f docker-compose-local.yaml up airflow-init
start-local:
	-mkdir plugins
	-mkdir logs
	docker-compose -f docker-compose-local.yaml up -d

debug-local:
	-mkdir plugins
	-mkdir logs
	docker-compose -f docker-compose-local.yaml up

stop-local:
	docker-compose -f docker-compose-local.yaml down