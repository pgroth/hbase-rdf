package nl.vu.datalayer.hbase.bulkload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public abstract class AbstractPrefixMatchBulkLoad {
	
	private static final long DEFAULT_BLOCK_SIZE = 134217728;
	private  HTableInterface string2Id = null;
	private  HTableInterface id2String = null;
	/**
	 * Cluster parameters used to estimate number of reducers 
	 */
	public  int CLUSTER_SIZE = 13;
	public  int TASK_PER_NODE = 2;
	/**
	 * Estimate of a quad size  
	 */
	
	public  int ELEMENTS_PER_QUAD = 4;
	/**
	 * Estimate of imbalance of data  distributed across reducers
	 * 1.0 = data equally distributed across reducers 
	 */
	public  double LOAD_BALANCER_FACTOR = 1.2;
	public  long totalStringCount;
	public  long numericalCount;
	public  long literalCount;
	public  long bNodeCount;
	public  int tripleToResourceReduceTasks;
	public  String schemaSuffix = "";
	protected  Path input;
	protected  int inputEstimateSize;
	protected  String outputPath;
	protected  byte rdfUnitType;
	protected  NativeJavaConnection con;
	protected  LoadIncrementalHFiles bulkLoad;
	protected FileSystem fs;
	protected long inputSplitSize;
	protected HBPrefixMatchSchema prefMatchSchema;

	public AbstractPrefixMatchBulkLoad(Path input, int inputEstimateSize, String outputPath, String schemaSuffix, boolean onlyTriples) {
		this.schemaSuffix = schemaSuffix;
		this.input = input;
		this.inputEstimateSize = inputEstimateSize;
		this.outputPath = outputPath;
		if (onlyTriples)
			rdfUnitType = RDFUnit.TRIPLE;
		else
			rdfUnitType = RDFUnit.QUAD;
	}

	protected void run() throws IOException, Exception, InterruptedException, ClassNotFoundException {
		long globalStartTime = System.currentTimeMillis();
		Path convertedTripletsPath = new Path(outputPath+ResourceToTriple.TEMP_TRIPLETS_DIR);
		Path idStringAssocInput = new Path(outputPath+"/"+QuadBreakDown.ID2STRING_DIR);
		
		con = (NativeJavaConnection)HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
		prefMatchSchema = createPrefixMatchSchema();
		prefMatchSchema.createCounterTable(con.getAdmin());
		
		Path resourceIds = new Path(outputPath+"/"+QuadBreakDown.RESOURCE_IDS_DIR);
		fs = FileSystem.get(con.getConfiguration());
		inputSplitSize = con.getConfiguration().getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
		
		runTripleToResourceJob(idStringAssocInput, resourceIds, prefMatchSchema);
		
		//SECOND PASS-----------------------------------------------	
		runResourceToTripleJob(resourceIds, convertedTripletsPath);
		
		bulkLoad = new LoadIncrementalHFiles(con.getConfiguration());
		
		bulkLoadIdStringMappingTables(idStringAssocInput);		
		bulkLoadQuadTables(convertedTripletsPath);
		
		con.close();
		long globalEndTime = System.currentTimeMillis();
		System.out.println("[Time] Total time: "+(globalEndTime-globalStartTime)+" ms");
		
		//TODO prefMatchSchema.warmUpBlockCache();
	}

	protected abstract void runResourceToTripleJob(Path resourceIds, Path convertedTripletsPath) throws IOException, InterruptedException, ClassNotFoundException;

	private void runTripleToResourceJob(Path idStringAssocInput, Path resourceIds, HBPrefixMatchSchema schema) throws IOException, InterruptedException, ClassNotFoundException {
		if (!fs.exists(resourceIds)) {
			long start = System.currentTimeMillis();
			Job j1 = createTripleToResourceJob(input, resourceIds, inputEstimateSize);
			j1.waitForCompletion(true);

			//move side effect files out of TripleToResource output directory
			moveIdStringAssocDirectory(resourceIds, idStringAssocInput);

			long firstJob = System.currentTimeMillis() - start;
			System.out.println("[Time] First pass finished in: " + firstJob + " ms");

			retrieveTripleToResourceCounters(j1);
			createSchemaFromCounters(schema);
		}
		else{//the output directory exists - we skip this job and read the counters from file
			createSchemaFromFile(schema);
		}
	}

	protected void createSchemaFromCounters(HBPrefixMatchSchema prefMatchSchema) throws IOException {
		long startPartition = HBPrefixMatchSchema.updateLastCounter(tripleToResourceReduceTasks, con.getConfiguration(), schemaSuffix)+1;
		//long startPartition = HBPrefixMatchSchema.getLastCounter(con.getConfiguration())+1;
		//System.out.println(totalStringCount+" : "+numericalCount);
		
		prefMatchSchema.setTableSplitInfo(totalStringCount, numericalCount, 
				tripleToResourceReduceTasks, startPartition, rdfUnitType==RDFUnit.TRIPLE);
		prefMatchSchema.create();
	}
	
	protected void createSchemaFromFile(HBPrefixMatchSchema prefMatchSchema) throws IOException {
		long startPartition = 0;
		System.out.println(totalStringCount+" : "+numericalCount);
		buildCountersFromFile();
		
		prefMatchSchema.setTableSplitInfo(totalStringCount, numericalCount, 
				tripleToResourceReduceTasks, startPartition, rdfUnitType==RDFUnit.TRIPLE);
		prefMatchSchema.create();
	}
	
	
	
	protected  final void bulkLoadIdStringMappingTables(Path idStringAssocInput) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		Path string2IdOutput = new Path(outputPath+StringIdAssoc.STRING2ID_DIR);
		Path id2StringOutput = new Path(outputPath+StringIdAssoc.ID2STRING_DIR);
		
		//String2Id PASS----------------------------------------------
		if (!fs.exists(string2IdOutput)) {
			long start = System.currentTimeMillis();
			Job j3 = createString2IdJob(con, idStringAssocInput, string2IdOutput);
			j3.waitForCompletion(true);
			System.out.println("[Time] String2Id MR finished in: " + (System.currentTimeMillis() - start));

			//string2Id = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);//TODO add when reading from file
			doTableBulkLoad(string2IdOutput, string2Id, con);
		}
		
		System.out.println("Finished bulk load for String2Id table");//=====================//=============
		
		if (!fs.exists(id2StringOutput)) {
			long start = System.currentTimeMillis();
			Job j4 = createId2StringJob(con, idStringAssocInput, id2StringOutput);
			j4.waitForCompletion(true);
			System.out.println("[Time] Id2String MR finished in: " + (System.currentTimeMillis() - start));

			//id2String = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);TODO add when reading from file
			doTableBulkLoad(id2StringOutput, id2String, con);
		} 
		System.out.println("Finished bulk load for Id2String table");//=====================//==================================
	}


	public  Job createTripleToResourceJob(Path input, Path output, int inputSizeEstimate) throws IOException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(inputSplitSize, 
													QuadBreakDown.QUAD_MEDIAN_SIZE, 
													QuadBreakDown.getMapOutputRecordSizeEstimate(), 1.4, 4);
		configureShuffle(conf, shuffleOptimizer);	
		
		conf.set("schemaSuffix", schemaSuffix);
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		Job j = new Job(conf);
		
		j.setJobName("TripleToResource");
		
		long numInputBytes = (long)(inputSizeEstimate*Math.pow(1024.0, 2.0));
		long numQuads = numInputBytes/RDFUnit.getAverageUnitSize(rdfUnitType);
		long totalNumberOfUniqueElements = numQuads*RDFUnit.getNumberOfAtoms(rdfUnitType);
		double maximumElementPerPartition = Math.pow(2.0, 24.0);
		double numPartitions = (double)totalNumberOfUniqueElements/maximumElementPerPartition;
		int taskEstimate = (int)(Math.ceil(numPartitions)* LOAD_BALANCER_FACTOR) ;
		tripleToResourceReduceTasks = CLUSTER_SIZE > taskEstimate ?
										CLUSTER_SIZE : taskEstimate;
		
		System.out.println("Number of reduce tasks: "+tripleToResourceReduceTasks);
		
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(QuadBreakDown.TripleToResourceMapper.class);
		j.setReducerClass(QuadBreakDown.TripleToResourceReducer.class);
		
		j.setOutputKeyClass(TypedId.class);
		j.setOutputValueClass(DataPair.class);
		
		j.setMapOutputKeyClass(HBaseValue.class);
		j.setMapOutputValueClass(DataPair.class);
	
		j.setInputFormatClass(TextInputFormat.class);
		j.setOutputFormatClass(SequenceFileOutputFormat.class);
		TextInputFormat.setInputPaths(j, input);
		SequenceFileOutputFormat.setOutputPath(j, output);
		
		j.setNumReduceTasks(tripleToResourceReduceTasks);
		
		return j;
	}

	public  Job createString2IdJob(HBaseConnection con, Path input, Path output) throws Exception {
		JobConf conf = new JobConf(); 
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(inputSplitSize, 
				QuadBreakDown.QUAD_MEDIAN_SIZE/4+BaseId.SIZE+Bytes.SIZEOF_INT+Bytes.SIZEOF_LONG, 
				StringIdAssoc.String2IdMapper.getMapOutputRecordSizeEstimate(), 1.4);
		configureShuffle(conf, shuffleOptimizer);
		conf.setInt("hbase.mapreduce.hfileoutputformat.blocksize", 8*1024);
		Job j = new Job(conf);
	
		j.setJobName(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(StringIdAssoc.String2IdMapper.class);
		j.setMapOutputKeyClass(ImmutableBytesWritable.class);
		j.setMapOutputValueClass(Put.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(HFileOutputFormat.class);
	
		SequenceFileInputFormat.setInputPaths(j, input);
		HFileOutputFormat.setOutputPath(j, output);
	
		((NativeJavaConnection)con).getConfiguration().setInt("hbase.rpc.timeout", 0);
		string2Id = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
	
		HFileOutputFormat.configureIncrementalLoad(j, (HTable)string2Id);
		return j;
	}

	public  Job createId2StringJob(HBaseConnection con, Path input, Path output) throws Exception {
		JobConf conf = new JobConf(); 
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		ShuffleStageOptimizer shuffleOptimizer = new ShuffleStageOptimizer(inputSplitSize, 
				QuadBreakDown.QUAD_MEDIAN_SIZE/4+BaseId.SIZE+Bytes.SIZEOF_INT+Bytes.SIZEOF_LONG, 
				StringIdAssoc.Id2StringMapper.getMapOutputRecordSizeEstimate(), 1.4);
		configureShuffle(conf, shuffleOptimizer);
		conf.setInt("hbase.mapreduce.hfileoutputformat.blocksize", 8*1024);
		Job j = new Job(conf);
	
		j.setJobName(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(StringIdAssoc.Id2StringMapper.class);
		j.setMapOutputKeyClass(ImmutableBytesWritable.class);
		j.setMapOutputValueClass(Put.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(HFileOutputFormat.class);
	
		SequenceFileInputFormat.setInputPaths(j, input);
		HFileOutputFormat.setOutputPath(j, output);
	
		((NativeJavaConnection)con).getConfiguration().setInt("hbase.rpc.timeout", 0);
		id2String = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
	
		HFileOutputFormat.configureIncrementalLoad(j, (HTable)id2String);
	
		return j;
	}

	public  void retrieveTripleToResourceCounters(Job j1) throws IOException {
		//sufixCounters = new TreeMap<Short, Long>();
		
		Counters counters = j1.getCounters();
		CounterGroup numGroup = counters.getGroup(QuadBreakDown.TripleToResourceReducer.NUMERICAL_GROUP);
		totalStringCount = numGroup.findCounter("NonNumericals").getValue();
		numericalCount = numGroup.findCounter("Numericals").getValue();
		
		CounterGroup elemsGroup = counters.getGroup(QuadBreakDown.TripleToResourceReducer.ELEMENT_TYPE_GROUP);
		literalCount = elemsGroup.findCounter("Literals").getValue();
		bNodeCount = elemsGroup.findCounter("Blanks").getValue();
		
		//build histogram
		/*CounterGroup histogramGroup = counters.getGroup(TripleToResource.TripleToResourceReducer.HISTOGRAM_GROUP);
		Iterator<Counter> it = histogramGroup.iterator();
		while (it.hasNext()){
			Counter c = it.next();
			short b = Short.parseShort(c.getName(), 16);
			//System.out.println((char)b +" "+c.getName()+" "+c.getValue());
			
			long half = c.getValue()/2;
			sufixCounters.put(b, half);
			sufixCounters.put((short)(b | 0x01), c.getValue()-half);
		}*/		
		
		//save counter values to file
		FileWriter file = new FileWriter("Counters");
		file.write(Long.toString(totalStringCount)+"\n");
		file.write(Long.toString(numericalCount)+"\n");
		file.write(Integer.toString(tripleToResourceReduceTasks)+"\n");
		
		/*for (Map.Entry<Short, Long> entry : sufixCounters.entrySet()) {
			file.write(entry.getKey()+"@"+entry.getValue()+"\n");
		}*/
		file.close();
	}

	public  void buildCountersFromFile() throws IOException {
		//sufixCounters = new TreeMap();
		FileReader file2 = new FileReader("Counters");
		BufferedReader reader = new BufferedReader(file2);
		totalStringCount = Long.parseLong(reader.readLine());
		numericalCount = Long.parseLong(reader.readLine());
		tripleToResourceReduceTasks = Integer.parseInt(reader.readLine());
		
		/*String line;
		long totalCounter = 0;
		while ((line=reader.readLine()) != null){
			String []tokens  = line.split("@");
			short s = Short.parseShort(tokens[0]);
			long current = Long.parseLong(tokens[1]);
			System.out.println("Short: "+s+"; Character: "+(char)s+"; "+current);
			sufixCounters.put(s, current);
			totalCounter += current;
		}
		System.out.println(totalCounter);*/
	}

	public  void moveIdStringAssocDirectory(Path resourceIds, Path id2StringInput) throws IOException {	
		Path source = new Path(resourceIds, QuadBreakDown.ID2STRING_DIR);
		fs.rename(source, id2StringInput);
	}

	protected  void doTableBulkLoad(Path dir, HTableInterface table, NativeJavaConnection con) throws InterruptedException, TableNotFoundException, IOException {
		long start = System.currentTimeMillis();
		bulkLoad.doBulkLoad(dir, (HTable) table);
		long bulkTime = System.currentTimeMillis() - start;
		System.out.println("[Time] "+table.getTableDescriptor().getNameAsString() + " bulkLoad time: " + bulkTime + " ms");
	}
	
	final protected void configureShuffle(Configuration conf, ShuffleStageOptimizer shuffleOptimizer) {
		conf.setInt("io.sort.mb", shuffleOptimizer.getIoSortMB());
		conf.setFloat("io.sort.spill.percent", shuffleOptimizer.getIoSortSpillThreshold());
		conf.setFloat("io.sort.record.percent", shuffleOptimizer.getIoSortRecordPercent());
		conf.setInt("io.sort.factor", shuffleOptimizer.getIoSortFactor());
	}

	/*public static long getIntermediateBufferSize(long inputSplitSizeBytes, int inputRecordSize, long mapOutRecordSize){		 		
		long inputRecordsPerSplit = inputSplitSizeBytes/inputRecordSize+1;
		return (inputRecordsPerSplit*mapOutRecordSize);
	}*/

	protected abstract HBPrefixMatchSchema createPrefixMatchSchema();
	
	protected abstract void bulkLoadQuadTables(Path convertedTripletsPath) throws Exception;

}
