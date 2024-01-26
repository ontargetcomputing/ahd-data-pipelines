clean-deploy:
	rm -rf conf
	rm -f databricks.yml

create-deployment-files: clean-deploy
	python bin/process_templates.py --env $(ENV)
	cp -rf conf.j2/workflows conf/workflows
	
validate: create-deployment-files
	databricks bundle validate

deploy: validate
	cp .gitignore .gitignore.orig
	cp .deployment_gitignore .gitignore
	databricks bundle deploy -t ${ENV} --profile prod 
	mv .gitignore.orig .gitignore