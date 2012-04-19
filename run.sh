#!/bin/bash
rm log
prun -v -no-panda -1 mvn 1 -Dlog4j.configFile=\"./log4j_config\" exec:java -Dexec.mainClass=\"nl.vu.datalayer.hbase.NTripleParser\" -Dexec.args=\"./data/tbl-card.ttl hexastore\" 
