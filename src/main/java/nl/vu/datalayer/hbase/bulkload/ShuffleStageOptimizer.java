package nl.vu.datalayer.hbase.bulkload;

public class ShuffleStageOptimizer {
	
	private static final int META_BUFFER_UNIT_SIZE = 16;
	public static final double DEFAULT_ESTIMATE_ERROR = 1.2;
	public static final float REDUCE_BUFFER_PERCENT = 0.7f;//assumes the reducers don't require too much memory i.e. don't hold state

	private long inputSplitSizeBytes;
	
	private long inputRecordSize;
	
	private long mapOutputRecordsSize;
	
	private long totalIoSortSizeBytes;
	
	private long ioSortSerializationBufferSize;
	
	private long ioSortMetaBufferSize;
	private int totalIoSortSizeMB;
	
	private double estimateError;
	
	private int inToOutCardinality;//number of output records corresponding to 1 input record
	
	private int childJVMSize;
	
	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, 
			long mapOutputRecordSize, int childJVMSize) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.inToOutCardinality = 1;
		this.estimateError = DEFAULT_ESTIMATE_ERROR;
		this.childJVMSize = childJVMSize;
		computeIoSortBufferSize();
	}
	
	

	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, 
													long mapOutputRecordSize,
													int cardinality,
													int childJVMSize) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.inToOutCardinality = cardinality;
		this.estimateError = DEFAULT_ESTIMATE_ERROR;
		this.childJVMSize = childJVMSize;
		computeIoSortBufferSize();
	}
	
	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, 
			long mapOutputRecordSize, double estimateError,
			int childJVMSize) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.estimateError = estimateError;
		this.inToOutCardinality = 1;
		this.childJVMSize = childJVMSize;
		computeIoSortBufferSize();
	}
	
	public ShuffleStageOptimizer(long inputSplitSizeBytes, long inputRecordSize, 
								long mapOutputRecordSize, double estimateError, 
								int cardinality, int childJVMSize) {
		super();
		this.inputSplitSizeBytes = inputSplitSizeBytes;
		this.inputRecordSize = inputRecordSize;
		this.mapOutputRecordsSize = mapOutputRecordSize;
		this.estimateError = estimateError;
		this.inToOutCardinality = cardinality;
		this.childJVMSize = childJVMSize;
		computeIoSortBufferSize();
	}
	
	final private void computeIoSortBufferSize(){
		long inputRecordsPerSplit = inputSplitSizeBytes/inputRecordSize+1;
		ioSortSerializationBufferSize = inputRecordsPerSplit*mapOutputRecordsSize;
		ioSortMetaBufferSize = inputRecordsPerSplit*META_BUFFER_UNIT_SIZE*inToOutCardinality;
		totalIoSortSizeBytes = ioSortMetaBufferSize+ioSortSerializationBufferSize;
		totalIoSortSizeMB = (int)(((double)totalIoSortSizeBytes*estimateError)/1024/1024);
		if (totalIoSortSizeMB > childJVMSize/2){
			totalIoSortSizeBytes/=2;
			ioSortMetaBufferSize/=2;
			totalIoSortSizeMB/=2;
			estimateError = 1.4;//we will spill at least twice
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
