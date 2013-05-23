Querying HBase Using SPARQL
---------------------------
May 24, 2013

HBase can be queried over a SPARQL endpoint by running the script runHBaseSparqlEngine.sh. This sets up a Jena Fuseki HTTP server using the configuration file fuseki-config-hbase.ttl .

Alternatively, queries can also be issued natively. An example of this is in examples.RunDBPediaBenchmark which reads the queries from a set of input files.


September 5, 2012

HBase can also be queried using a Sail API that accepts most types of SPARQL queries.

A set of example queries can be found in the _data/test-queries.rq_ file.

The script _sparql.sh_ can be used to send queries to HBase, either by using the standard input, or by providing a query file, where each query is on a separate line. To run this script on the example query file, build the project and then type:

	$ ./sparql.sh "-f DEFAULT"

