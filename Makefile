.PHONY: test jar code-coverage run clean

test:
	mvn clean checkstyle:check test

jar:
	mvn clean checkstyle:check package

jar-no-test:
	mvn -Dmaven.test.skip=true clean checkstyle:check package

code-coverage:
	mvn clean clover2:setup test clover2:aggregate clover2:clover

run: jar
	@java -Djavax.security.auth.useSubjectCredsOnly=false \
		-Dlog4j.configuration=file:src/main/resources/dev-log4j.properties \
   		-jar target/cubed.jar \
	   	--version v0.0.0 \
		--schema-files-dir src/test/resources/schemas/ \
		--db-config-file src/main/resources/database-configuration.properties

run-no-test: 
	@java -Djavax.security.auth.useSubjectCredsOnly=false \
		-Dlog4j.configuration=file:src/main/resources/dev-log4j.properties \
   		-jar target/cubed.jar \
	   	--version v0.0.0 \
		--schema-files-dir src/test/resources/schemas/ \
		--db-config-file src/main/resources/database-configuration.properties

clean:
	mvn clean
