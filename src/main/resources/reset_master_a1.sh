#!/bin/bash
MANAGIX_HOME=/Users/heri/asterix-master-mgmt
ASTERIX_DIR=/Users/heri/git/asterix-feed

${MANAGIX_HOME}/bin/managix stop -n a1
${MANAGIX_HOME}/bin/managix delete -n a1
${MANAGIX_HOME}/bin/managix create -n a1 -c ${MANAGIX_HOME}/clusters/local/local.xml

if [ "$#" -eq  1 ]
   then
 	echo "Installing new libs (Adapters and exterenal functions)"
	${MANAGIX_HOME}/bin/managix stop -n a1
	${MANAGIX_HOME}/bin/managix install -n a1 -d feeds -l kbalib -p ${ASTERIX_DIR}/target/asterix-external-lib-zip-binary-assembly.zip
	${MANAGIX_HOME}/bin/managix start -n a1
	echo "Copying necessary resource files and directory"
	cp ${ASTERIX_DIR}/src/main/resources/name_variants_lookup.json ${MANAGIX_HOME}/clusters/local/working_dir/
	cp -r ${ASTERIX_DIR}/src/main/resources/profiles ${MANAGIX_HOME}/clusters/local/working_dir/
fi
