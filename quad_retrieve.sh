#!/bin/bash
mvn exec:java -Dexec.mainClass="nl.vu.datalayer.hbase.RetrieveQuads" -Dexec.args="quad_queries native-java" 

