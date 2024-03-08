clean-deploy:
	rm -rf conf
	rm -f databricks.yml

create-deployment-files: clean-deploy
	python bin/process_templates.py --env $(ENV)
	cp -rf conf.j2/workflows conf/workflows
	
validate: create-deployment-files
	databricks bundle validate -t ${ENV} --profile ${ENV}

deploy: validate
	cp .gitignore .gitignore.orig
	cp .deployment_gitignore .gitignore
	databricks bundle deploy -t ${ENV} --profile ${ENV}
	mv .gitignore.orig .gitignore

venv:
	poetry shell

unit-test:
	pytest

unit-test-cov:
	pytest --cov=src --cov-fail-under=14.60
