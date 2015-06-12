#!/bin/bash
usage()
{
cat << EOF
usage: $0 [restart|stop] options

This script run re-install a new copy of Asterix (managix)

OPTIONS:
   -o      	Optional - run maven offline to avoid downloading "xml" metadata files (same as mvn -o)
   -c|--clean   Optional - run clean before building the package
   -m|--managixdir   Managix directory
   -a|-- asterixsrc   Asterix source code directory
   -d|--asterixdev  Home directory for external feed adapters and UDF development
EOF
}

ASTERIX_SOURCE=/Users/heri/Work/Workspace/Experimental/asterix_feeds/asterixdb
ASTERIX_EXTERNAL_DEV_HOME=/Users/heri/git/asterix-feed
ONLINE=
CLEAN=

while [[ $# > 0 ]]
do
key="$1"

case $key in
    -o|--online)
    ONLINE="-o"
    #shift # past argument
    ;;
    -c|--clean)
    CLEAN="mvn clean &&"
    #shift # past argument
    ;;
    -m|--managixdir)
    MANAGIX_HOME="$2"
    if [ -z $MANAGIX_HOME ]
    then
    	usage
    	exit 1
    fi
    shift # past argument
    ;;
    -d|--asterixdev)
    ASTERIX_DEV_HOME="$2"
    if [ -z ASTERIX_DEV_HOME ]
    then
    	usage
    	exit 1
    fi
    shift # past argument
    ;;
    -s|--asterixsrc)
    ASTERIX_SOURCE="$2"
    if [ -z $ASTERIX_SOURCE ]
    then
    	usage
    	exit 1
    fi
    shift # past argument
    ;;
    --default)
    DEFAULT=YES
    ;;
    *)
    	usage
    	exit 1
            # unknown option
    ;;
esac
shift # past argument or value
done

ASTERIX_EXTERNAL_DEV_RESOURCES=${ASTERIX_EXTERNAL_DEV_HOME}/src/main/resources

echo ASTERIX_EXTERNAL_DEV_RESOURCES = ${ASTERIX_EXTERNAL_DEV_RESOURCES}
echo ASTERIX_SOURCE = ${ASTERIX_SOURCE}
echo "MAVEN = mvn clean && mvn ${ONLINE} package -DskipTests"

${MANAGIX_HOME}/bin/managix stop -n a1
${MANAGIX_HOME}/bin/managix delete -n a1

echo "Cleaning up"
rm -rf ${MANAGIX_HOME}/*
rm -rf ${MANAGIX_HOME}/.*

echo "Recompiling Asterix"
cd ${ASTERIX_SOURCE}
mvn clean && mvn ${ONLINE} package -DskipTests

echo "Installing new Asterix Copy"
cp ${ASTERIX_SOURCE}/asterix-installer/target/asterix-installer-0.8.6-SNAPSHOT-binary-assembly.zip ${MANAGIX_HOME}/
cd ${MANAGIX_HOME}
unzip asterix-installer-0.8.6-SNAPSHOT-binary-assembly.zip
cp ${ASTERIX_EXTERNAL_DEV_RESOURCES}/reset_a1.sh ./
${MANAGIX_HOME}/bin/managix configure
cp ${ASTERIX_EXTERNAL_DEV_RESOURCES}/AsterixManagement/asterix-configuration.xml ./conf
cp ${ASTERIX_EXTERNAL_DEV_RESOURCES}/AsterixManagement/local.xml ./clusters/local/

echo "Cleaning up"
rm asterix-installer-0.8.6-SNAPSHOT-binary-assembly.zip
