package nl.vu.datalayer.hbase.exceptions;

public class NumericalRangeException extends Exception {

	private static final long serialVersionUID = 148584607226955296L;

	public NumericalRangeException() {
		super();
	}

	public NumericalRangeException(String message) {
		super(message);
	}
	
}
