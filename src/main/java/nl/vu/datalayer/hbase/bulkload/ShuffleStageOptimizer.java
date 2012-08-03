package nl.vu.datalayer.hbase.bulkload;

public class ShuffleStageOptimizer {
	
	private static final int META_BUFFER_UNIT_SIZE = 16;
	public static final double DEFAULT_ESTIMATE_ERROR = 1.2;
	public static final int CHILD_JVM_SIZE = 1024;

	private long inputSplitSizeBytes;
	
	private long inputRecordSize;
	
	private long mapOutputRecordsSize;
	
	private long totalIoSortSizeBytes;
	
	private long ioSortSerializationBufferSize;
	
	private long ioSortMetaBufferSize;
	private int totalIoSortSizeMB;
	
	private double estimateError;
	
	private int inToOutCardinality;//number of output records corresponding to 1 input record
	
	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, long mapOutputRecordSize) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.inToOutCardinality = 1;
		this.estimateError = DEFAULT_ESTIMATE_ERROR;

		computeIoSortBufferSize();
	}
	
	

	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, 
													long mapOutputRecordSize,
													int cardinality) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.inToOutCardinality = cardinality;
		this.estimateError = DEFAULT_ESTIMATE_ERROR;
		
		computeIoSortBufferSize();
	}
	
	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, long mapOutputRecordSize, double estimateError) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.estimateError = estimateError;
		this.inToOutCardinality = 1;

		computeIoSortBufferSize();
	}
	
	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, 
								long mapOutputRecordSize, double estimateError, int cardinality) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.estimateError = estimateError;
		this.inToOutCardinality = cardinality;
		
		computeIoSortBufferSize();
	}
	
	final private void computeIoSortBufferSize(){
		long inputRecordsPerSplit = inputSplitSizeBytes/inputRecordSize+1;
		ioSortSerializationBufferSize = inputRecordsPerSplit*mapOutputRecordsSize;
		ioSortMetaBufferSize = inputRecordsPerSplit*META_BUFFER_UNIT_SIZE*inToOutCardinality;
		totalIoSortSizeBytes = ioSortMetaBufferSize+ioSortSerializationBufferSize;
		totalIoSortSizeMB = (int)(((double)totalIoSortSizeBytes*estimateError)/1024/1024);
		if (totalIoSortSizeMB > CHILD_JVM_SIZE/2){
			totalIoSortSizeBytes/=2;
			ioSortMetaBufferSize/=2;
			totalIoSortSizeMB/=2;
			estimateError = DEFAULT_ESTIMATE_ERROR;
		}
	}

	public int getIoSortMB() {
		return totalIoSortSizeMB;
	}
	
	public float getIoSortSpillThreshold(){
		if (estimateError == DEFAULT_ESTIMATE_ERROR){
			return 1.0f;//the estimate is accurate enough so we are confident we are not going to spill
		}
		else{//the estimate is rough, there is a good chance we will spill 
			return 0.8f;
		}
	}
	
	public float getIoSortRecordPercent(){
		return (float)ioSortMetaBufferSize/(float)totalIoSortSizeBytes;
	}
	
	public int getIoSortFactor(){
		return totalIoSortSizeMB/10+1;
	}
	
}
