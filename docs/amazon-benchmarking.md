Running benchmarks on Amazon EC2
--------------------------------

1. Berlin SPARQL Benchmark (BSBM)

	./runHBaseSparqlEngine.sh &
	./testdriver -runs 10 -w 2 -t 3600000 http://localhost:3030/ds/sparql

2. DBPedia SPARQL Benchmark

	mvn exec:java -Dexec.mainClass="examples.RunDBPediaBenchmark" -Dexec.args="<directory_with_dbpedia_queries> 2 10"


