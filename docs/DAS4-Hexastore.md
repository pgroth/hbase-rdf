April 02 2012
------------------
Scripts:
- run.sh - creates the Hexastore schema (if not already created) and populates the tables
			from the input file
- retrieve.sh - queries HBase Hexastore tables with queries from the input file
- queries     - example input file for retrieve.sh
- log4j_config - configuration file for log4j

------------------

Class Architecture

An HBaseClientSolution comprises a schema and a util object that handles information 
exchange with the database based on that schema.
Any schema class will implement the interface "IHBaseSchema".
Any util class will implement the interface "IHBaseUtil".

2 schemas have been implemented so far:
- "predicate-cf" - uses a modification of predicates as column family names
				 - the keys are the raw subjects
				 - the values are the raw objects
				 - uses 2 tables - one for storing triplets
				 				 - one for mappings between column family names and predicates
				 
- "hexastore"    - uses 7 tables: "S-PO", "P-SO", "O-SP",  "SO-P", "SP-O", "PO-S", "SPO" 
				 - the primary key encoding is based on an 8-byte hash of a triple element
				 	e.g. in the table "SO-P" the key is 16 bytes, the first 8 bytes
				 		are the hash of S, the last 8 bytes the hash of O
				 - the value is stored in raw format
				 - any triplet should appear in all 7 tables

The "HBaseFactory" will be used to retrieve HBaseClientSolution objects based on a schema name. 

"NTripleParser" is the main class for populating the tables with triples.
"RetrieveTriples" and "RetrieveURI" are the main classes for populating the tables with triples.