#!/bin/bash
managix stop -n a1
managix delete -n a1
managix create -n a1 -c ${MANAGIX_HOME}/clusters/local/local.xml

if [ "$#" -eq  1 ]
   then
 	echo "Installing new libs (Adapters and exterenal functions)"
	managix stop -n a1
	managix install -n a1 -d feeds -l kbalib -p /Users/heri/git/asterix-feed/target/asterix-external-lib-zip-binary-assembly.zip
	managix start -n a1
	cp /Users/heri/git/asterix-feed/src/main/resources/name_variants_lookup.json ${MANAGIX_HOME}/clusters/local/working_dir/
fi
