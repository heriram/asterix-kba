# asterix-kba
Development of KBA-based UDFs and External feeds for AsterixDB
This project is for developing Knowledge Base Acceleration modules using AsterixDB @ UC Irvine as a platform

## Maven
To run import in Eclipse, run the following first:

	mvn eclipse:eclipse -DASTERIX_SOURCE=path/to/asterixdb-source-code

Before building the package you will need to edit the `config.properities` file which you can find [here](https://github.com/heriram/asterix-kba/blob/master/src/main/resources/config.properties).

To build the package so that you can load the externa library you need to run this:

	mvn package -DskipTests -DASTERIX_SOURCE=path/to/asterixdb-source-code
	
After this is finished, the created assemby zip-file can be loaded to Asterix 
(see also Asterix external library usage for more information)
