package nl.vu.datalayer.hbase.exceptions;

public class NumericalRangeException extends Exception {

	static final long serialVersionUID = 1L;

	public NumericalRangeException() {
		super();
	}

	public NumericalRangeException(String message) {
		super(message);
	}
	
}
