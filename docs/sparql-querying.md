Querying HBase Using SPARQL
---------------------------
September 5, 2012

HBase can be queried using a Sail API that accepts most types of SPARQL queries.

A set of example queries can be found in the _data/test-queries.rq_ file.

The script _sparql.sh_ can be used to send queries to HBase, either by using the standard input, or by providing a query file, where each query is on a separate line. To run this script on the example query file, build the project and then type:

	$ ./sparql.sh "-f DEFAULT"

