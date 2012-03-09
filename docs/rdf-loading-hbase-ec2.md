Loading RDF in HBase
====================
March 9, 2012

Once you've started up hbase somewhere, you might want to start loading some RDF. Using the current code, we have here's how you do it.

         # Make sure git and maven are installed
         # put the apt-get commands here
         # get the code
         $ git clone git://github.com/pgroth/hbase-rdf.git
         
         # build the program
         $ make install

         # run the loader
         mvn exec:java -Dexec.mainClass="nl.vu.datalayer.hbase.NTripleParser" -Dexec.args="./data/tbl-card.ttl"

Note: Need to check this stuff for hbase configuration and make sure we support arguments properly 
          

     