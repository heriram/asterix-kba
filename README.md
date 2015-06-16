# Asterix-kba
Development of KBA-based UDFs and External feeds for AsterixDB
This project is for developing Knowledge Base Acceleration modules using AsterixDB @ UC Irvine as a platform

This code requires jdk 7 or newer to compile/build.

## Maven
To run import in Eclipse, run the following first:

	mvn eclipse:eclipse -DASTERIX_SOURCE=path/to/asterixdb-source-code

Before building the package you will need to edit the `config.properities` file which you can find [here](https://github.com/heriram/asterix-kba/blob/master/src/main/resources/config.properties).

To build the package so that you can load the externa library you need to run this:

	mvn package -DskipTests -DASTERIX_SOURCE=path/to/asterixdb-source-code
	
After this is finished, the created assemby zip-file can be loaded to Asterix 
(see also Asterix external library usage for more information)

## Reinstalling a new Managix
When you have made any changes on the AsterixDB code, you can run the [`reinstall_asterix.sh`](https://github.com/heriram/asterix-kba/blob/master/reinstall_asterix.sh) script as, e.g., follows:

	./reinstall_asterix.sh -m /Users/username/asterix-mgnt -o -s /Users/username/Work/Asterix/incubator-asterixdb -d 	/Users/username/Work/Asterix/asterix-kba -v 0.8.6

For usage, run `reinstall_asterix.sh -h`

## Updates on External Adapters or UDFs
The above script will copy `reset_a1.sh` script into `${MANAGIX_HOME}` (or to where you intalled managix) which you can use to re-update the external libraries etc.

When you have made any changes on the asterix-kba code, you can run `reset_a1.sh` as follows:

	../reset_a1.sh -n a1 -i -s /Users/username/Work/Asterix/asterix-kba -m /Users/username/asterix_mgnt

For help run `reset_a1.sh -h`
