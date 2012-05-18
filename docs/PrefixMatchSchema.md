-------Schema structure -------

The PrefixMatch schema has a total of 8 tables. It encodes anything other than numerical literals to 8-byte ids, while for numerical literals we store the equivalent java representation. 
Numerical literals can only appear in object positions, so only in these positions we prepend a type byte (to a total of 9 bytes) which has the following structure:
- first bit - 0 - nonNumerical (string)
	    - 1 - numerical 
- next 4 bits - 16 types of XSD numericals

The links between elements are kept in 6 tables: SPOC, POCS, OCSP, CSPO, CPSO, OSPC. The entire quad is stored in the key of each table, so the key length is 3*8 + 9 = 33 bytes. The cell value stored is empty for these keys. This allows to use HBase's key prefix matching feature to support multiple types of queries on the same table 
e.g. the table SPOC will support the queries S??? ; SP?? ; SPO? and SPOC. 

2 additional tables are needed for mapping nonNumericals (including URIs, bNodes and nonNumerical Literals) to Ids : String2Id, Id2String. 
The maximum length of a row key in HBase is limited to 32768, but for the table String2Id we can have strings larger than that. The solution was to hash only these large strings to an 8-byte hash and add a non-RDF rdf character to make sure there are no collisions with other entries in the table. Furthermore for efficient bulk loading (explained in the next section) in String2Id we store the strings in reverse order. 
For Id2String we don't have any restrictions on the size of the cell value, so we store the Strings as they are provided by our NQuads parser.

------ Bulk Loading ------------

The entire bulk loading process is made up of a MR pass per table (so 8 passes), plus 2 additional passes for generating unique Ids.

In the first pass:
- mapper 
	  - generates an 8-byte quadID - 5 bytes from partitionID + 3 bytes local counter
	  - breaks the quads into its composing elements
	  - emits (elementString, <quadID, elem position in quad>)

- reducer
	  - generates an typed elementID associated to the elementString: 
		- for nonNumericals the id is generated from partitionId + localCounter (like in the mapper)
		- for numericals we use the internal java representation truncated to 8-bytes
	  - emits (elementID, <quadID+position in quad>)
	  - for nonNumerical Ids writes a separate SequenceFile with the mapping <elementID, elementString>
	  - count numericals and nonNumericals
	  - count the number of occurences of the last byte in each string (for literals we disregard the ending \") using Hadoop counters; since Hadoop limits the number of counters to 120 we use 1 counter for 2 consecutive bytes

In the second pass:
- mapper
	  - swap elementId with quadId and emmit (quadId, <elementId+position in quad>)
- reducer
	  - aggregate the quad elements into an ImmutableBytesWritable object in the order SPOC
	  - emmit (NullWritable, ImmutableBytesWritable) in SequenceFiles


For each table we run a MR job that will generate files in internal HBase format. The number of reducers for these jobs is automatically configured using HBase's bulkload API: the number of reducers is equal to the number of regions in a table. As a result each table has to be pre-split, specifying for each region the key subspace that it will handle. In order for the MR table load to be as efficient as possible, the splits have to be made so that the input is divided equally between reducers.

After each MR job that generates internal HBase files, we call LoadIncrementalFile.doBulkLoad which will esentially tell HBase to start adopting the files generated in the prevous MR job. This process might take more than 10 minutes if some region server will have much more data to load than others. If this is the case our client will timeout and throw an IOException. However the region servers will continue the bulk loading operation after the client times out. So our approach was to catch the IOException thrown by the client and wait another 5 minutes for the servers to continue with the bulk loading process, after which we move on to the next MR job.

The String2Id mapper reads the SequenceFiles generated in the first pass and emmits (reversedString, elmentId). The strings are reversed because in this way they offer a better dispersion in the key space. If we were to use the normal strings, the majority would be URIs starting with "http://" and they would all fall in the same region, so they would all go to the same reducer. Furthermore we improved the splitting of reversed strings by computing a histogram of the last characters in the strings during the first MR pass. We then use this histogram to make more splits for characters that appear at the end of a greater number of strings.
For example: let's say 3000 elements end in 'a', and 1000 end in 'b' and we want to split the key space between "a\x00\x00\x00" and "b\xff\xff\xff" in equal parts. Then the resulting splits would be: "a\x55\x55\x55", "a\xaa\xaa\xaa", "b\x00\x00\x00". 

The Id2String mapper also reads the SequenceFiles generated in the first pass and emits (elmentId, elementString). The splits are made depending on the number of reduce partitions from the first MR pass, because the partition Id represents the first 5 bytes of the elementId. So for 112(= \x70) partitions our maximum generated id is \x00\x00\x00\x00\x69\xff\xff\xff. So we divide this space into equal parts, making the assumption that ids are well distributed across partitions.

For the 6 tables that store the quads we read the Sequence Files generated from the 2nd MR pass and rearrange the elements from SPOC order to the order associated with each table. The tables SPOC, POCS, CSPO and CPSO have all have keys prefixed by a non-numerical id, so the splits used for the Id2String table are also well suited here. The tables OCSP and OSPC can also have a numerical prefix, which all have the first bit 1. So the numericals fall into a separate key subspace from nonNumerical Ids. To make the splits well balanced, we count the nonNumericals and numericals during the first MR pass using Hadoop user counters.


-------Implementation details ------------------------

nl.vu.datalayer.hbase.RetrieveQuads - example on how to retrieve quads with the current PrefixSchema
nl.vu.datalayer.hbase.util.HBPrefixMatchUtil - retrieval functionality
nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema - functionality for creating tables and computing appropriate splits
nl.vu.datalayer.hbase.bulkload package - contains classes for all MR jobs involved in bulkloading the PrefixMatch schema
nl.vu.datalayer.hbase.id - contains classes exposing Ids and data wrappers used in the MR jobs







