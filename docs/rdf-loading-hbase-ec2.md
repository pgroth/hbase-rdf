Loading RDF in HBase
====================
May 21, 2013

Once you've started up hbase somewhere, you might want to start loading some RDF. Using the current code, we have here's how you do it.

         # Make sure git and maven are installed
         # put the apt-get commands here
         # get the code
         $ git clone git://github.com/pgroth/hbase-rdf.git
         
         # build the program
         $ mvn install

	 # edit config.properties

         # run the loader
         $hadoop jar target/hbase-0.0.1-SNAPSHOT-jar-with-dependencies.jar nl.vu.datalayer.hbase.bulkload.BulkLoad <hdfs_input_dir> <hdfs_input_size_in_MB> <hdfs_output_dir>


          

     
