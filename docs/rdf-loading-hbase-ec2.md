Loading RDF in HBase on EC2
====================
May 24, 2013

1. Prepare the data in an HDFS directory. The data should be uncompressed in ntriples/nquads format.
2. Run a "hadoop dfs -du" command on the input directory to get the total input size in bytes.
3. Get the code and build the jar

         $ git clone git://github.com/pgroth/hbase-rdf.git
         $ mvn install -DskipTests=true

4. Edit config.properties :
* schema_suffix -> string attached at the end of all table names
* only_triples -> true/false boolean 
* slave_nodes -> integer specifying number of slave nodes in the HBase/Hadoop cluster
* engine_caching -> on/off should be on for BSBM ; off for DBPedia

5. Add the hbase-site.xml config file to the jar
         
         $ cd target
         $ cp <location_of_hbase-site.xml> hbase-site.xml
         $ jar -uf hbase-0.0.1-SNAPSHOT-jar-with-dependencies.jar hbase-site.xml

6. Run the bulk loader. Pass the input data directory, the size from 2 converted to MB and the output HDFS directory (should not exist).
         
         $ hadoop jar target/hbase-0.0.1-SNAPSHOT-jar-with-dependencies.jar nl.vu.datalayer.hbase.bulkload.BulkLoad <hdfs_input_dir> <hdfs_input_size_in_MB> <hdfs_output_dir>


          



