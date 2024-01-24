validate:
	databricks bundle validate

deploy:
	databricks bundle deploy -t ${ENV} --profile prod