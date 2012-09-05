#!/bin/bash
mvn exec:java -Dexec.mainClass="nl.vu.datalayer.hbase.SPARQLQuery" -Dexec.args="$@" 

