# Created By Enrique Plata

SHELL = /bin/bash

.DEFAULT_GOAL := help

.PHONY: setup
setup: ## 		0.- Setup environment
	@ conda env create -f conda_env.yml

.PHONY: load_raw
load_raw: ## 		2.- Running load_raw data
	@ cd ./steps/ && python 02_load_raw.py

.PHONY: create_pos_view
create_pos_view: ## 		4.- Running create_pos_view
	@ cd ./steps/ && python 04_create_pos_view.py

.PHONY: fahrenheit_to_celsius
fahrenheit_to_celsius: ## 		05.- Running fahrenheit_to_celsius locally
	@ cd ./steps/05_fahrenheit_to_celsius_udf && python app.py 35

.PHONY: fahrenheit_to_celsius_deploy
fahrenheit_to_celsius_deploy: ## 		05.- Deploy fahrenheit_to_celsius
	@ cd ./steps/05_fahrenheit_to_celsius_udf && snow function create

.PHONY: orders_udate_sp
orders_udate_sp: ## 		06.- Running orders_udate_sp locally
	@ cd ./steps/06_orders_update_sp && python app.py

.PHONY: orders_udate_sp_deploy
orders_udate_sp_deploy: ## 		06.- Deploey orders_udate_sp
	@ cd ./steps/06_orders_update_sp && snow procedure create

.PHONY: daily_city_metrics_update_sp
daily_city_metrics_update_sp: ## 		07.- Running daily_city_metrics_update_sp
	@ cd ./steps/07_daily_city_metrics_update_sp && python app.py

.PHONY: daily_city_metrics_update_sp_deploy
daily_city_metrics_update_sp_deploy: ## 	07.- Running daily_city_metrics_update_sp
	@ cd ./steps/07_daily_city_metrics_update_sp && snow procedure create

help:
	@ echo "Please use \`make <target>' where <target> is one of"
	@ perl -nle'print $& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-25s\033[0m %s\n", $$1, $$2}'
