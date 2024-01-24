clean-deploy:
	rm -rf conf
	rm databricks.yml

prepare-for-creating-deployment-files:
	python bin/convert_tasks_templates.py --env $(ENV)
	
create-deployment-files: clean-deploy prepare-for-creating-deployment-files
	jinja -d conf.j2/data.$(ENV).yml -f yaml -o databricks.yml databricks.j2.yml

validate:
	databricks bundle validate

deploy:
	databricks bundle deploy -t ${ENV} --profile prod