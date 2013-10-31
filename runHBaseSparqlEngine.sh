#!/bin/bash
java -Xmx2048M -cp lib/fuseki-server.jar:lib/asynchbase-1.5.0-SNAPSHOT.jar:target/hbase-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.apache.jena.fuseki.FusekiCmd --config=fuseki-config-hbase.ttl
